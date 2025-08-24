"""OSM data seeding using Overpass API."""

import asyncio
import time
from pathlib import Path
from typing import Dict, List, Optional

import httpx
from slugify import slugify

from .config import settings
from .log import logger
from .utils import Timer


# City coordinates for Overpass queries
CITY_COORDS = {
    "blr": {"lat": 12.9716, "lon": 77.5946, "radius": 15000, "name": "Bangalore"},
    "del": {"lat": 28.6139, "lon": 77.2090, "radius": 20000, "name": "Delhi"},
    "mum": {"lat": 19.0760, "lon": 72.8777, "radius": 20000, "name": "Mumbai"},
    "chen": {"lat": 13.0827, "lon": 80.2707, "radius": 15000, "name": "Chennai"},
    "hyd": {"lat": 17.3850, "lon": 78.4867, "radius": 15000, "name": "Hyderabad"},
    "pune": {"lat": 18.5204, "lon": 73.8567, "radius": 12000, "name": "Pune"},
    "kol": {"lat": 22.5726, "lon": 88.3639, "radius": 15000, "name": "Kolkata"},
}


class OverpassClient:
    """Client for Overpass API to fetch OSM restaurant data."""
    
    def __init__(self):
        self.base_url = "https://overpass-api.de/api/interpreter"
        self.timeout = httpx.Timeout(60.0)  # Overpass can be slow
    
    async def query_restaurants(
        self, 
        lat: float, 
        lon: float, 
        radius: int = 5000,
        limit: Optional[int] = None
    ) -> List[Dict[str, any]]:
        """Query restaurants from OSM using Overpass API."""
        
        # Build Overpass query
        query = f"""
        [out:json][timeout:60];
        (
          node["amenity"="restaurant"](around:{radius},{lat},{lon});
          way["amenity"="restaurant"](around:{radius},{lat},{lon});
          relation["amenity"="restaurant"](around:{radius},{lat},{lon});
        );
        out center meta;
        """
        
        logger.info(
            "Querying Overpass API",
            lat=lat,
            lon=lon, 
            radius=radius,
            limit=limit
        )
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                with Timer("overpass_query") as timer:
                    response = await client.post(
                        self.base_url,
                        data=query,
                        headers={
                            "User-Agent": settings.user_agent,
                            "Content-Type": "text/plain"
                        }
                    )
                    response.raise_for_status()
                
                data = response.json()
                logger.info(
                    "Overpass query completed",
                    elements_found=len(data.get("elements", [])),
                    duration=timer.elapsed
                )
                
                return self._process_overpass_results(data, limit)
                
        except Exception as e:
            logger.error("Overpass API query failed", error=str(e))
            raise
    
    def _process_overpass_results(
        self, 
        data: Dict[str, any], 
        limit: Optional[int] = None
    ) -> List[Dict[str, any]]:
        """Process Overpass API results into restaurant records."""
        elements = data.get("elements", [])
        restaurants = []
        
        for element in elements:
            try:
                # Get coordinates (handle nodes vs ways/relations)
                if element["type"] == "node":
                    lat, lon = element["lat"], element["lon"]
                elif "center" in element:
                    lat, lon = element["center"]["lat"], element["center"]["lon"]
                else:
                    logger.warning("No coordinates found for element", osm_id=element.get("id"))
                    continue
                
                tags = element.get("tags", {})
                name = tags.get("name")
                
                if not name:
                    logger.debug("Skipping restaurant without name", osm_id=element.get("id"))
                    continue
                
                # Extract relevant tags
                restaurant = {
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "osm_id": element.get("id"),
                    "osm_type": element.get("type"),
                    "website": tags.get("website") or tags.get("contact:website") or tags.get("url"),
                    "phone": tags.get("phone") or tags.get("contact:phone"),
                    "address": self._build_address_from_tags(tags),
                    "cuisine": tags.get("cuisine"),
                    "opening_hours": tags.get("opening_hours"),
                    "source": "osm",
                }
                
                restaurants.append(restaurant)
                
            except Exception as e:
                logger.warning(
                    "Failed to process OSM element", 
                    osm_id=element.get("id"),
                    error=str(e)
                )
                continue
        
        # Apply limit if specified
        if limit and len(restaurants) > limit:
            restaurants = restaurants[:limit]
            logger.info("Limited results", requested=limit, found=len(restaurants))
        
        return restaurants
    
    def _build_address_from_tags(self, tags: Dict[str, str]) -> Optional[str]:
        """Build address string from OSM tags."""
        addr_parts = []
        
        # House number and street
        house_number = tags.get("addr:housenumber")
        street = tags.get("addr:street")
        
        if house_number and street:
            addr_parts.append(f"{house_number} {street}")
        elif street:
            addr_parts.append(street)
        
        # Additional address components
        for key in ["addr:locality", "addr:city", "addr:state"]:
            value = tags.get(key)
            if value:
                addr_parts.append(value)
        
        # Postal code
        postcode = tags.get("addr:postcode")
        if postcode:
            addr_parts.append(postcode)
        
        return ", ".join(addr_parts) if addr_parts else None


async def seed_city(city_slug: str, limit: Optional[int] = None) -> List[Dict[str, any]]:
    """Seed restaurant data for a city using OSM."""
    if city_slug not in CITY_COORDS:
        raise ValueError(f"Unknown city: {city_slug}. Available: {list(CITY_COORDS.keys())}")
    
    city_info = CITY_COORDS[city_slug]
    logger.info("Starting OSM seed", city=city_info["name"], city_slug=city_slug)
    
    # Rate limiting for politeness
    await asyncio.sleep(1.0)  # Be polite to Overpass API
    
    client = OverpassClient()
    restaurants = await client.query_restaurants(
        lat=city_info["lat"],
        lon=city_info["lon"],
        radius=city_info["radius"],
        limit=limit
    )
    
    logger.info(
        "OSM seeding completed",
        city=city_info["name"],
        restaurants_found=len(restaurants)
    )
    
    return restaurants


def seed_from_file(file_path: Path) -> List[Dict[str, any]]:
    """Seed restaurant data from CSV file (for offline demo)."""
    import csv
    import json
    
    restaurants = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Parse JSON fields
                cuisines = json.loads(row.get("cuisines", "[]"))
                hours = json.loads(row.get("hours_json", "{}"))
                
                restaurant = {
                    "name": row["name"],
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "phone": row.get("phone"),
                    "address": row.get("address_full"),
                    "website": row.get("website"),
                    "cuisine": ",".join(cuisines) if cuisines else None,
                    "opening_hours": None,  # We have structured hours instead
                    "pincode": row.get("pincode"),
                    "source": "gold_file",
                    "cuisines_structured": cuisines,
                    "hours_structured": hours,
                }
                
                restaurants.append(restaurant)
        
        logger.info("Loaded restaurants from file", file_path=str(file_path), count=len(restaurants))
        
    except Exception as e:
        logger.error("Failed to load restaurants from file", file_path=str(file_path), error=str(e))
        raise
    
    return restaurants
