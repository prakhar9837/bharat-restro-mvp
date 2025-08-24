"""Export restaurant data to CSV and JSON formats."""

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .config import settings
from .log import logger
from .persist import db_manager


def export_to_csv(
    output_path: Optional[Path] = None,
    limit: Optional[int] = None
) -> Path:
    """Export restaurant data to CSV format."""
    
    if not output_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = settings.export_dir / f"restaurants_{timestamp}.csv"
    
    # Ensure export directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting CSV export", output_path=str(output_path))
    
    # Get restaurant data
    restaurants = db_manager.get_all_restaurants(limit=limit)
    
    if not restaurants:
        logger.warning("No restaurants found for export")
        return output_path
    
    # Define CSV columns
    fieldnames = [
        'restaurant_id',
        'canonical_name',
        'address_full',
        'pincode',
        'lat',
        'lon',
        'phone',
        'website',
        'cuisines',
        'hours',
        'updated_at'
    ]
    
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for restaurant in restaurants:
                row = restaurant.to_dict()
                
                # Convert lists/dicts to JSON strings for CSV
                if row.get('cuisines') and isinstance(row['cuisines'], list):
                    row['cuisines'] = json.dumps(row['cuisines'])
                
                if row.get('hours') and isinstance(row['hours'], dict):
                    row['hours'] = json.dumps(row['hours'])
                
                writer.writerow(row)
        
        logger.info("CSV export completed", 
                   output_path=str(output_path), 
                   restaurants_count=len(restaurants))
        
        return output_path
        
    except Exception as e:
        logger.error("CSV export failed", output_path=str(output_path), error=str(e))
        raise


def export_to_json(
    output_path: Optional[Path] = None,
    limit: Optional[int] = None,
    pretty: bool = True
) -> Path:
    """Export restaurant data to JSON format."""
    
    if not output_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = settings.export_dir / f"restaurants_{timestamp}.json"
    
    # Ensure export directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting JSON export", output_path=str(output_path))
    
    # Get restaurant data
    restaurants = db_manager.get_all_restaurants(limit=limit)
    
    if not restaurants:
        logger.warning("No restaurants found for export")
        # Create empty export
        export_data = {
            "metadata": {
                "exported_at": datetime.utcnow().isoformat(),
                "total_restaurants": 0,
                "export_format": "json"
            },
            "restaurants": []
        }
    else:
        # Convert to dictionaries
        restaurant_dicts = [restaurant.to_dict() for restaurant in restaurants]
        
        export_data = {
            "metadata": {
                "exported_at": datetime.utcnow().isoformat(),
                "total_restaurants": len(restaurant_dicts),
                "export_format": "json"
            },
            "restaurants": restaurant_dicts
        }
    
    try:
        with open(output_path, 'w', encoding='utf-8') as jsonfile:
            if pretty:
                json.dump(export_data, jsonfile, indent=2, ensure_ascii=False)
            else:
                json.dump(export_data, jsonfile, ensure_ascii=False)
        
        logger.info("JSON export completed", 
                   output_path=str(output_path), 
                   restaurants_count=len(restaurants))
        
        return output_path
        
    except Exception as e:
        logger.error("JSON export failed", output_path=str(output_path), error=str(e))
        raise


def export_provenance_data(
    output_path: Optional[Path] = None,
    restaurant_id: Optional[str] = None
) -> Path:
    """Export provenance data to JSON format."""
    
    if not output_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{restaurant_id}" if restaurant_id else ""
        output_path = settings.export_dir / f"provenance{suffix}_{timestamp}.json"
    
    # Ensure export directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info("Starting provenance export", output_path=str(output_path))
    
    try:
        with db_manager.get_session() as session:
            from .persist import Provenance
            
            query = session.query(Provenance)
            if restaurant_id:
                query = query.filter(Provenance.restaurant_id == restaurant_id)
            
            provenance_records = query.all()
            
            # Convert to dictionaries
            provenance_data = [record.to_dict() for record in provenance_records]
            
            export_data = {
                "metadata": {
                    "exported_at": datetime.utcnow().isoformat(),
                    "total_records": len(provenance_data),
                    "restaurant_id": restaurant_id,
                    "export_format": "provenance_json"
                },
                "provenance": provenance_data
            }
            
            with open(output_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(export_data, jsonfile, indent=2, ensure_ascii=False)
            
            logger.info("Provenance export completed", 
                       output_path=str(output_path), 
                       records_count=len(provenance_data))
            
            return output_path
            
    except Exception as e:
        logger.error("Provenance export failed", output_path=str(output_path), error=str(e))
        raise


def export_summary_stats() -> Dict[str, any]:
    """Generate summary statistics of the database."""
    
    logger.info("Generating summary statistics")
    
    try:
        with db_manager.get_session() as session:
            from .persist import Restaurant, Provenance
            from sqlalchemy import func
            
            # Basic counts
            total_restaurants = session.query(Restaurant).count()
            total_provenance = session.query(Provenance).count()
            
            # Restaurants with specific fields
            with_phone = session.query(Restaurant).filter(Restaurant.phone.isnot(None)).count()
            with_website = session.query(Restaurant).filter(Restaurant.website.isnot(None)).count()
            with_coordinates = session.query(Restaurant).filter(
                Restaurant.lat.isnot(None),
                Restaurant.lon.isnot(None)
            ).count()
            with_hours = session.query(Restaurant).filter(Restaurant.hours.isnot(None)).count()
            with_cuisines = session.query(Restaurant).filter(Restaurant.cuisines.isnot(None)).count()
            
            # Coverage percentages
            coverage = {}
            if total_restaurants > 0:
                coverage = {
                    "phone": round(with_phone / total_restaurants * 100, 1),
                    "website": round(with_website / total_restaurants * 100, 1),
                    "coordinates": round(with_coordinates / total_restaurants * 100, 1),
                    "hours": round(with_hours / total_restaurants * 100, 1),
                    "cuisines": round(with_cuisines / total_restaurants * 100, 1),
                }
            
            # Average confidence by field
            confidence_stats = {}
            confidence_fields = ['address_full', 'phone', 'hours', 'cuisines']
            
            for field in confidence_fields:
                avg_confidence = session.query(func.avg(Provenance.confidence)).filter(
                    Provenance.field == field
                ).scalar()
                
                if avg_confidence:
                    confidence_stats[field] = round(float(avg_confidence), 3)
            
            stats = {
                "generated_at": datetime.utcnow().isoformat(),
                "totals": {
                    "restaurants": total_restaurants,
                    "provenance_records": total_provenance,
                },
                "field_counts": {
                    "phone": with_phone,
                    "website": with_website,
                    "coordinates": with_coordinates,
                    "hours": with_hours,
                    "cuisines": with_cuisines,
                },
                "coverage_percentage": coverage,
                "average_confidence": confidence_stats,
            }
            
            logger.info("Summary statistics generated", total_restaurants=total_restaurants)
            
            return stats
            
    except Exception as e:
        logger.error("Failed to generate summary statistics", error=str(e))
        raise


def export_data(format_type: str, limit: Optional[int] = None) -> Path:
    """Export data in specified format."""
    
    format_type = format_type.lower()
    
    if format_type == "csv":
        return export_to_csv(limit=limit)
    elif format_type == "json":
        return export_to_json(limit=limit)
    else:
        raise ValueError(f"Unsupported export format: {format_type}")


def export_all_formats(limit: Optional[int] = None) -> Dict[str, Path]:
    """Export data in all supported formats."""
    
    logger.info("Exporting in all formats", limit=limit)
    
    results = {}
    
    try:
        results["csv"] = export_to_csv(limit=limit)
        results["json"] = export_to_json(limit=limit)
        
        # Also export summary stats
        stats = export_summary_stats()
        stats_path = settings.export_dir / f"summary_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        results["stats"] = stats_path
        
        logger.info("All format exports completed", formats=list(results.keys()))
        
        return results
        
    except Exception as e:
        logger.error("Export all formats failed", error=str(e))
        raise
