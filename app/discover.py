"""Website discovery from OSM data and curated sources."""

import csv
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from .config import settings
from .log import logger


class WebsiteDiscoverer:
    """Discover official websites for restaurants."""
    
    def __init__(self):
        self.curated_sites = {}
        self._load_curated_sites()
    
    def _load_curated_sites(self) -> None:
        """Load curated website mappings from CSV."""
        curated_file = settings.data_dir / "curated_sites.csv"
        
        if not curated_file.exists():
            logger.info("No curated sites file found", path=str(curated_file))
            return
        
        try:
            with open(curated_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    name_key = self._normalize_name(row.get("name", ""))
                    website = row.get("website", "").strip()
                    
                    if name_key and website:
                        self.curated_sites[name_key] = website
            
            logger.info("Loaded curated sites", count=len(self.curated_sites))
            
        except Exception as e:
            logger.error("Failed to load curated sites", error=str(e))
    
    def discover_website(self, restaurant: Dict[str, any]) -> Optional[str]:
        """Discover website for a restaurant."""
        
        # 1. Check OSM tags first
        website = self._extract_from_osm_tags(restaurant)
        if website:
            logger.debug("Found website in OSM tags", name=restaurant.get("name"), website=website)
            return website
        
        # 2. Check curated mappings
        website = self._lookup_curated_site(restaurant)
        if website:
            logger.debug("Found website in curated sites", name=restaurant.get("name"), website=website)
            return website
        
        # 3. Future: Could add search engine discovery here
        
        logger.debug("No website found", name=restaurant.get("name"))
        return None
    
    def _extract_from_osm_tags(self, restaurant: Dict[str, any]) -> Optional[str]:
        """Extract website from OSM tags."""
        # Direct website field
        website = restaurant.get("website")
        if website and self._is_valid_url(website):
            return website
        
        # No other OSM sources in our simplified data structure
        return None
    
    def _lookup_curated_site(self, restaurant: Dict[str, any]) -> Optional[str]:
        """Look up website in curated mappings."""
        name = restaurant.get("name", "")
        if not name:
            return None
        
        name_key = self._normalize_name(name)
        return self.curated_sites.get(name_key)
    
    def _normalize_name(self, name: str) -> str:
        """Normalize restaurant name for matching."""
        import re
        
        # Convert to lowercase
        name = name.lower()
        
        # Remove common prefixes/suffixes
        prefixes = ["the ", "hotel ", "restaurant "]
        suffixes = [" restaurant", " hotel", " cafe", " dhaba", " bar"]
        
        for prefix in prefixes:
            if name.startswith(prefix):
                name = name[len(prefix):]
                break
        
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break
        
        # Remove special characters, keep only alphanumeric and spaces
        name = re.sub(r'[^a-z0-9\s]', '', name)
        
        # Collapse multiple spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid."""
        try:
            parsed = urlparse(url)
            return bool(parsed.netloc and parsed.scheme in ('http', 'https'))
        except Exception:
            return False


def discover_websites(restaurants: List[Dict[str, any]]) -> List[Dict[str, any]]:
    """Discover websites for a list of restaurants."""
    discoverer = WebsiteDiscoverer()
    
    discovered_count = 0
    
    for restaurant in restaurants:
        website = discoverer.discover_website(restaurant)
        if website:
            restaurant["website"] = website
            discovered_count += 1
    
    logger.info(
        "Website discovery completed",
        total_restaurants=len(restaurants),
        websites_found=discovered_count
    )
    
    return restaurants


# Example curated sites CSV format:
def create_sample_curated_sites() -> None:
    """Create a sample curated sites CSV file."""
    curated_file = settings.data_dir / "curated_sites.csv"
    
    if curated_file.exists():
        return
    
    sample_data = [
        {"name": "Saravana Bhavan", "website": "https://saravanabhavan.com"},
        {"name": "Pind Balluchi", "website": "https://pindballuchi.com"},
        {"name": "China Gate", "website": "https://chinagate.in"},
        {"name": "Cafe Coffee Day", "website": "https://cafecoffeeday.com"},
        {"name": "Toit", "website": "https://toit.in"},
        {"name": "Moti Mahal", "website": "https://motimahal.in"},
        {"name": "Theobroma", "website": "https://theobroma.in"},
        {"name": "Gajalee", "website": "https://gajalee.com"},
    ]
    
    try:
        with open(curated_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["name", "website"])
            writer.writeheader()
            writer.writerows(sample_data)
        
        logger.info("Created sample curated sites file", path=str(curated_file))
        
    except Exception as e:
        logger.error("Failed to create curated sites file", error=str(e))
