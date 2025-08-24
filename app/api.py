"""FastAPI web interface for Bharat Resto MVP."""

import asyncio
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import persist, export, seed, validate, eval as evaluation
from .cli import _run_pipeline
from .config import settings
from .log import logger

# Initialize FastAPI app
app = FastAPI(
    title="Bharat Resto MVP API",
    description="AI-powered Indian restaurant data extraction pipeline",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files (for frontend)
frontend_path = Path(__file__).parent.parent / "frontend"
if frontend_path.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_path)), name="static")

# Background task storage
pipeline_tasks: Dict[str, Dict] = {}

# Pydantic models
class RestaurantResponse(BaseModel):
    restaurant_id: str
    canonical_name: str
    address_full: Optional[str] = None
    pincode: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    cuisines: List[str] = []
    hours: Dict[str, str] = {}
    updated_at: Optional[datetime] = None

class PipelineRequest(BaseModel):
    city: str = Field(..., description="City code (blr, del, mum, etc.)")
    limit: Optional[int] = Field(50, ge=1, le=1000, description="Maximum restaurants to process")
    llm_enabled: bool = Field(True, description="Enable LLM-based extraction")
    concurrency: int = Field(4, ge=1, le=10, description="Concurrent fetching")

class PipelineStatus(BaseModel):
    task_id: str
    status: str  # "running", "completed", "failed"
    progress: float  # 0.0 to 1.0
    message: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    results: Optional[Dict] = None

class StatusResponse(BaseModel):
    total_restaurants: int
    total_provenance: int
    field_coverage: Dict[str, Dict[str, Any]]
    database_path: str
    last_updated: Optional[datetime] = None

class ValidationResponse(BaseModel):
    total_restaurants: int
    valid_restaurants: int
    invalid_restaurants: int
    validation_issues: List[Dict[str, Any]]

# Database dependency
def get_db():
    """Get database manager instance."""
    persist.db_manager.init_db()
    return persist.db_manager

# API Routes

@app.get("/")
async def root():
    """Root endpoint - serve frontend or API info."""
    frontend_file = Path(__file__).parent.parent / "frontend" / "index.html"
    if frontend_file.exists():
        return FileResponse(str(frontend_file))
    return {
        "message": "Bharat Resto MVP API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "/status"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "database": "connected" if settings.db_path.exists() else "not_found"
    }

@app.get("/restaurants", response_model=List[RestaurantResponse])
async def get_restaurants(
    city: Optional[str] = Query(None, description="Filter by city"),
    cuisine: Optional[str] = Query(None, description="Filter by cuisine"),
    has_phone: Optional[bool] = Query(None, description="Filter restaurants with phone numbers"),
    has_website: Optional[bool] = Query(None, description="Filter restaurants with websites"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db = Depends(get_db)
):
    """Get restaurants with optional filtering."""
    try:
        restaurants = db.get_all_restaurants()
        
        # Apply filters
        filtered = []
        for restaurant in restaurants:
            # City filter (basic implementation - could be enhanced)
            if city and city.lower() not in (restaurant.canonical_name or "").lower():
                continue
                
            # Cuisine filter
            if cuisine:
                restaurant_cuisines = restaurant.cuisines or []
                if isinstance(restaurant_cuisines, str):
                    try:
                        restaurant_cuisines = json.loads(restaurant_cuisines)
                    except:
                        restaurant_cuisines = []
                if not any(cuisine.lower() in c.lower() for c in restaurant_cuisines):
                    continue
            
            # Phone filter
            if has_phone is not None:
                has_phone_value = bool(restaurant.phone)
                if has_phone != has_phone_value:
                    continue
            
            # Website filter
            if has_website is not None:
                has_website_value = bool(restaurant.website)
                if has_website != has_website_value:
                    continue
            
            filtered.append(restaurant)
        
        # Apply pagination
        paginated = filtered[offset:offset + limit]
        
        # Convert to response model
        return [
            RestaurantResponse(
                restaurant_id=r.restaurant_id,
                canonical_name=r.canonical_name,
                address_full=r.address_full,
                pincode=r.pincode,
                lat=r.lat,
                lon=r.lon,
                phone=r.phone,
                website=r.website,
                cuisines=json.loads(r.cuisines) if r.cuisines else [],
                hours=json.loads(r.hours) if r.hours else {},
                updated_at=r.updated_at
            )
            for r in paginated
        ]
        
    except Exception as e:
        logger.error("Failed to get restaurants", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get restaurants: {str(e)}")

@app.get("/restaurants/{restaurant_id}", response_model=RestaurantResponse)
async def get_restaurant(restaurant_id: str, db = Depends(get_db)):
    """Get specific restaurant by ID."""
    try:
        restaurants = db.get_all_restaurants()
        restaurant = next((r for r in restaurants if r.restaurant_id == restaurant_id), None)
        
        if not restaurant:
            raise HTTPException(status_code=404, detail="Restaurant not found")
        
        return RestaurantResponse(
            restaurant_id=restaurant.restaurant_id,
            canonical_name=restaurant.canonical_name,
            address_full=restaurant.address_full,
            pincode=restaurant.pincode,
            lat=restaurant.lat,
            lon=restaurant.lon,
            phone=restaurant.phone,
            website=restaurant.website,
            cuisines=json.loads(restaurant.cuisines) if restaurant.cuisines else [],
            hours=json.loads(restaurant.hours) if restaurant.hours else {},
            updated_at=restaurant.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get restaurant", restaurant_id=restaurant_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get restaurant: {str(e)}")

@app.post("/pipeline/run")
async def run_pipeline(
    request: PipelineRequest,
    background_tasks: BackgroundTasks
):
    """Trigger the extraction pipeline in the background."""
    task_id = str(uuid.uuid4())
    
    # Initialize task status
    pipeline_tasks[task_id] = {
        "task_id": task_id,
        "status": "running",
        "progress": 0.0,
        "message": "Pipeline started",
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "results": None
    }
    
    # Add background task
    background_tasks.add_task(
        run_pipeline_background,
        task_id,
        request.city,
        request.limit,
        request.llm_enabled,
        request.concurrency
    )
    
    return {"task_id": task_id, "status": "started", "message": "Pipeline execution started"}

async def run_pipeline_background(
    task_id: str,
    city: str,
    limit: Optional[int],
    llm_enabled: bool,
    concurrency: int
):
    """Run pipeline in background task."""
    try:
        # Update task status
        pipeline_tasks[task_id]["message"] = "Initializing pipeline"
        pipeline_tasks[task_id]["progress"] = 0.1
        
        # Update configuration
        settings.llm_enabled = llm_enabled
        
        # Initialize database
        persist.db_manager.init_db()
        
        pipeline_tasks[task_id]["message"] = "Running pipeline"
        pipeline_tasks[task_id]["progress"] = 0.2
        
        # Run the pipeline
        await _run_pipeline(city, limit, None, concurrency)
        
        # Get results
        restaurants = persist.db_manager.get_all_restaurants()
        
        pipeline_tasks[task_id].update({
            "status": "completed",
            "progress": 1.0,
            "message": f"Pipeline completed successfully. Processed {len(restaurants)} restaurants.",
            "completed_at": datetime.utcnow(),
            "results": {
                "restaurants_processed": len(restaurants),
                "city": city,
                "limit": limit,
                "llm_enabled": llm_enabled
            }
        })
        
    except Exception as e:
        logger.error("Pipeline failed", task_id=task_id, error=str(e))
        pipeline_tasks[task_id].update({
            "status": "failed",
            "progress": 0.0,
            "message": f"Pipeline failed: {str(e)}",
            "completed_at": datetime.utcnow(),
            "results": None
        })

@app.get("/pipeline/status/{task_id}", response_model=PipelineStatus)
async def get_pipeline_status(task_id: str):
    """Get status of a pipeline execution."""
    if task_id not in pipeline_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = pipeline_tasks[task_id]
    return PipelineStatus(**task)

@app.get("/pipeline/tasks")
async def get_all_pipeline_tasks():
    """Get all pipeline tasks."""
    return list(pipeline_tasks.values())

@app.get("/status", response_model=StatusResponse)
async def get_status(db = Depends(get_db)):
    """Get overall system status."""
    try:
        stats = export.export_summary_stats()
        
        return StatusResponse(
            total_restaurants=stats["totals"]["restaurants"],
            total_provenance=stats["totals"]["provenance_records"],
            field_coverage=stats["field_counts"],
            database_path=str(settings.db_path),
            last_updated=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error("Failed to get status", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")

@app.post("/validate", response_model=ValidationResponse)
async def validate_data(db = Depends(get_db)):
    """Validate all restaurant data in database."""
    try:
        restaurants = db.get_all_restaurants()
        
        valid_count = 0
        validation_issues = []
        
        for restaurant in restaurants:
            restaurant_data = restaurant.to_dict()
            valid, issues = validate.validate_restaurant_data(restaurant_data)
            
            if valid:
                valid_count += 1
            else:
                validation_issues.append({
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.canonical_name,
                    "issues": issues
                })
        
        return ValidationResponse(
            total_restaurants=len(restaurants),
            valid_restaurants=valid_count,
            invalid_restaurants=len(restaurants) - valid_count,
            validation_issues=validation_issues
        )
        
    except Exception as e:
        logger.error("Validation failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

@app.get("/export/{format}")
async def export_data(
    format: str,
    limit: Optional[int] = Query(None, description="Limit number of records")
):
    """Export restaurant data in specified format."""
    if format not in ["csv", "json"]:
        raise HTTPException(status_code=400, detail="Format must be 'csv' or 'json'")
    
    try:
        output_path = export.export_data(format, limit=limit)
        return FileResponse(
            path=str(output_path),
            filename=f"restaurants_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{format}",
            media_type="application/octet-stream"
        )
        
    except Exception as e:
        logger.error("Export failed", format=format, error=str(e))
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

@app.get("/cities")
async def get_supported_cities():
    """Get list of supported cities."""
    return {
        "cities": [
            {"code": "blr", "name": "Bengaluru", "country": "India"},
            {"code": "del", "name": "Delhi", "country": "India"},
            {"code": "mum", "name": "Mumbai", "country": "India"},
            {"code": "che", "name": "Chennai", "country": "India"},
            {"code": "hyd", "name": "Hyderabad", "country": "India"},
            {"code": "pun", "name": "Pune", "country": "India"},
            {"code": "kol", "name": "Kolkata", "country": "India"},
            {"code": "ahm", "name": "Ahmedabad", "country": "India"},
            {"code": "jai", "name": "Jaipur", "country": "India"},
            {"code": "koc", "name": "Kochi", "country": "India"}
        ]
    }

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"detail": "Endpoint not found", "path": str(request.url.path)}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error("Internal server error", path=str(request.url.path), error=str(exc))
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )