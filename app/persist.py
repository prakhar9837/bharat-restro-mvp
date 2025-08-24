"""Database models and persistence layer using SQLAlchemy."""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import (
    Column, 
    Integer, 
    String, 
    REAL, 
    Text, 
    DateTime,
    ForeignKey,
    create_engine,
    select
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker, relationship

from .config import settings
from .log import logger

Base = declarative_base()


class Restaurant(Base):
    """Restaurant table model."""
    
    __tablename__ = "restaurants"
    
    restaurant_id = Column(String, primary_key=True)
    canonical_name = Column(String, nullable=False)
    address_full = Column(Text, nullable=True)
    pincode = Column(String, nullable=True)
    lat = Column(REAL, nullable=True)
    lon = Column(REAL, nullable=True)
    phone = Column(String, nullable=True)
    website = Column(String, nullable=True)
    cuisines = Column(Text, nullable=True)  # JSON-encoded list
    hours = Column(Text, nullable=True)     # JSON-encoded dict
    updated_at = Column(String, nullable=False)  # ISO8601
    
    # Relationship to provenance records
    provenance_records = relationship("Provenance", back_populates="restaurant")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert restaurant to dictionary."""
        return {
            "restaurant_id": self.restaurant_id,
            "canonical_name": self.canonical_name,
            "address_full": self.address_full,
            "pincode": self.pincode,
            "lat": self.lat,
            "lon": self.lon,
            "phone": self.phone,
            "website": self.website,
            "cuisines": json.loads(self.cuisines) if self.cuisines else [],
            "hours": json.loads(self.hours) if self.hours else {},
            "updated_at": self.updated_at,
        }


class Provenance(Base):
    """Provenance table for tracking data sources and confidence."""
    
    __tablename__ = "provenance"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    restaurant_id = Column(String, ForeignKey("restaurants.restaurant_id"), nullable=False)
    field = Column(String, nullable=False)       # e.g., 'address_full'
    value = Column(Text, nullable=False)         # canonical stored value
    confidence = Column(REAL, nullable=False)
    source_url = Column(String, nullable=True)
    content_hash = Column(String, nullable=True)
    model_name = Column(String, nullable=True)
    model_version = Column(String, nullable=True)
    extracted_at = Column(String, nullable=False)  # ISO8601
    
    # Relationship to restaurant
    restaurant = relationship("Restaurant", back_populates="provenance_records")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert provenance to dictionary."""
        return {
            "id": self.id,
            "restaurant_id": self.restaurant_id,
            "field": self.field,
            "value": self.value,
            "confidence": self.confidence,
            "source_url": self.source_url,
            "content_hash": self.content_hash,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "extracted_at": self.extracted_at,
        }


class DatabaseManager:
    """Database connection and operations manager."""
    
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or settings.db_path
        self.engine = None
        self.SessionLocal = None
        self._initialized = False
    
    def init_db(self) -> None:
        """Initialize database connection and create tables."""
        if self._initialized:
            return
            
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create engine
        db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(
            db_url,
            connect_args={"check_same_thread": False},
            echo=False
        )
        
        # Create tables
        Base.metadata.create_all(bind=self.engine)
        
        # Create session factory
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        self._initialized = True
        logger.info("Database initialized", db_path=str(self.db_path))
    
    def get_session(self) -> Session:
        """Get database session."""
        if not self._initialized:
            self.init_db()
        return self.SessionLocal()
    
    def upsert_restaurant(
        self,
        restaurant_data: Dict[str, Any],
        provenance_data: List[Dict[str, Any]]
    ) -> str:
        """Upsert restaurant and provenance records."""
        with self.get_session() as session:
            try:
                # Generate or use existing restaurant ID
                restaurant_id = restaurant_data.get("restaurant_id")
                if not restaurant_id:
                    restaurant_id = str(uuid.uuid4())
                    restaurant_data["restaurant_id"] = restaurant_id
                
                # Set updated timestamp
                restaurant_data["updated_at"] = datetime.utcnow().isoformat()
                
                # JSON encode lists/dicts
                if "cuisines" in restaurant_data and isinstance(restaurant_data["cuisines"], list):
                    restaurant_data["cuisines"] = json.dumps(restaurant_data["cuisines"])
                
                if "hours" in restaurant_data and isinstance(restaurant_data["hours"], dict):
                    restaurant_data["hours"] = json.dumps(restaurant_data["hours"])
                
                # Upsert restaurant
                existing = session.get(Restaurant, restaurant_id)
                if existing:
                    # Update existing
                    for key, value in restaurant_data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    restaurant = existing
                else:
                    # Create new
                    restaurant = Restaurant(**restaurant_data)
                    session.add(restaurant)
                
                # Add provenance records
                for prov_data in provenance_data:
                    prov_data["restaurant_id"] = restaurant_id
                    prov_data["extracted_at"] = datetime.utcnow().isoformat()
                    
                    provenance = Provenance(**prov_data)
                    session.add(provenance)
                
                session.commit()
                logger.info("Restaurant upserted", restaurant_id=restaurant_id)
                
                return restaurant_id
                
            except Exception as e:
                session.rollback()
                logger.error("Failed to upsert restaurant", error=str(e))
                raise
    
    def get_restaurant(self, restaurant_id: str) -> Optional[Restaurant]:
        """Get restaurant by ID."""
        with self.get_session() as session:
            return session.get(Restaurant, restaurant_id)
    
    def search_restaurants(
        self,
        name: Optional[str] = None,
        city: Optional[str] = None,
        cuisine: Optional[str] = None,
        limit: int = 100
    ) -> List[Restaurant]:
        """Search restaurants with filters."""
        with self.get_session() as session:
            query = select(Restaurant)
            
            if name:
                query = query.where(Restaurant.canonical_name.ilike(f"%{name}%"))
            
            if city:
                query = query.where(Restaurant.address_full.ilike(f"%{city}%"))
            
            if cuisine:
                query = query.where(Restaurant.cuisines.ilike(f"%{cuisine}%"))
            
            query = query.limit(limit)
            
            result = session.execute(query)
            return result.scalars().all()
    
    def get_all_restaurants(self, limit: Optional[int] = None) -> List[Restaurant]:
        """Get all restaurants."""
        with self.get_session() as session:
            query = select(Restaurant)
            if limit:
                query = query.limit(limit)
            
            result = session.execute(query)
            return result.scalars().all()


# Global database manager instance
db_manager = DatabaseManager()
