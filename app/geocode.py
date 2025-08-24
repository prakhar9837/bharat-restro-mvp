"""Geocoding using Nominatim with SQLite caching."""

import json
import sqlite3
import time
from pathlib import Path
from typing import Optional, Tuple

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded

from .config import settings
from .log import logger
from .utils import hash_content


class GeocodingCache:
    """SQLite-based cache for geocoding results."""
    
    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_cache_db()
    
    def _init_cache_db(self) -> None:
        """Initialize the cache database."""
        conn = sqlite3.connect(self.cache_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS geocode_cache (
                    address_hash TEXT PRIMARY KEY,
                    address_text TEXT NOT NULL,
                    latitude REAL,
                    longitude REAL,
                    response_json TEXT,
                    cached_at REAL NOT NULL,
                    success INTEGER NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cached_at ON geocode_cache(cached_at)
            """)
            conn.commit()
        finally:
            conn.close()
    
    def get(self, address: str) -> Optional[Tuple[Optional[float], Optional[float], dict]]:
        """Get cached geocoding result."""
        address_hash = hash_content(address.lower().strip())
        
        conn = sqlite3.connect(self.cache_path)
        try:
            cursor = conn.execute(
                "SELECT latitude, longitude, response_json, success FROM geocode_cache WHERE address_hash = ?",
                (address_hash,)
            )
            row = cursor.fetchone()
            
            if row:
                lat, lon, response_json, success = row
                response = json.loads(response_json) if response_json else {}
                
                logger.debug("Cache hit for geocoding", address=address)
                
                if success:
                    return lat, lon, response
                else:
                    return None, None, response
            
            return None
            
        finally:
            conn.close()
    
    def set(
        self, 
        address: str, 
        latitude: Optional[float], 
        longitude: Optional[float], 
        response: dict,
        success: bool
    ) -> None:
        """Cache geocoding result."""
        address_hash = hash_content(address.lower().strip())
        
        conn = sqlite3.connect(self.cache_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO geocode_cache 
                (address_hash, address_text, latitude, longitude, response_json, cached_at, success)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                address_hash,
                address,
                latitude,
                longitude,
                json.dumps(response),
                time.time(),
                1 if success else 0
            ))
            conn.commit()
            
            logger.debug("Cached geocoding result", address=address, success=success)
            
        finally:
            conn.close()
    
    def cleanup_old_entries(self, max_age_days: int = 30) -> None:
        """Remove old cache entries."""
        cutoff_time = time.time() - (max_age_days * 24 * 60 * 60)
        
        conn = sqlite3.connect(self.cache_path)
        try:
            cursor = conn.execute(
                "DELETE FROM geocode_cache WHERE cached_at < ?",
                (cutoff_time,)
            )
            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                logger.info("Cleaned up old geocoding cache entries", deleted_count=deleted_count)
                
        finally:
            conn.close()


class GeocodeService:
    """Geocoding service with caching and rate limiting."""
    
    def __init__(self):
        self.geocoder = Nominatim(
            user_agent=settings.user_agent,
            timeout=10
        )
        self.cache = GeocodingCache(settings.geocode_cache)
        self.last_request_time = 0.0
        self.min_request_interval = 1.0  # Nominatim requires 1 request per second
    
    async def geocode_address(self, address: str) -> Tuple[Optional[float], Optional[float]]:
        """Geocode an address to lat/lon coordinates."""
        if not address or not address.strip():
            return None, None
        
        address = address.strip()
        logger.debug("Geocoding address", address=address)
        
        # Check cache first
        cached_result = self.cache.get(address)
        if cached_result is not None:
            lat, lon, response = cached_result
            return lat, lon
        
        # Rate limiting for Nominatim
        await self._apply_rate_limit()
        
        try:
            # Geocode with Nominatim
            location = self.geocoder.geocode(
                address,
                exactly_one=True,
                limit=1,
                addressdetails=True,
                extratags=True
            )
            
            if location:
                lat, lon = location.latitude, location.longitude
                
                # Validate coordinates are in India
                if self._is_in_india(lat, lon):
                    # Cache successful result
                    self.cache.set(
                        address, 
                        lat, 
                        lon, 
                        {"address": location.address, "raw": location.raw},
                        success=True
                    )
                    
                    logger.info("Successfully geocoded address", address=address, lat=lat, lon=lon)
                    return lat, lon
                else:
                    logger.warning("Geocoded coordinates outside India", address=address, lat=lat, lon=lon)
                    # Cache unsuccessful result
                    self.cache.set(address, None, None, {"error": "Outside India bounds"}, success=False)
                    return None, None
            else:
                logger.warning("No geocoding results found", address=address)
                # Cache unsuccessful result
                self.cache.set(address, None, None, {"error": "No results found"}, success=False)
                return None, None
                
        except GeocoderTimedOut:
            logger.error("Geocoding request timed out", address=address)
            return None, None
            
        except GeocoderQuotaExceeded:
            logger.error("Geocoding quota exceeded", address=address)
            # Cache to avoid repeated requests
            self.cache.set(address, None, None, {"error": "Quota exceeded"}, success=False)
            return None, None
            
        except Exception as e:
            logger.error("Geocoding failed", address=address, error=str(e))
            return None, None
    
    async def _apply_rate_limit(self) -> None:
        """Apply rate limiting for Nominatim API."""
        import asyncio
        
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            delay = self.min_request_interval - time_since_last
            logger.debug("Rate limiting geocoding request", delay=delay)
            await asyncio.sleep(delay)
        
        self.last_request_time = time.time()
    
    def _is_in_india(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within India bounds."""
        # India approximate bounds
        return 6.0 <= lat <= 38.0 and 68.0 <= lon <= 98.0
    
    def cleanup_cache(self) -> None:
        """Clean up old cache entries."""
        self.cache.cleanup_old_entries()


# Global geocoding service instance
geocode_service = GeocodeService()


async def geocode_address(address: str) -> Tuple[Optional[float], Optional[float]]:
    """Geocode an address to coordinates."""
    return await geocode_service.geocode_address(address)


def cleanup_geocoding_cache() -> None:
    """Clean up old geocoding cache entries."""
    geocode_service.cleanup_cache()
