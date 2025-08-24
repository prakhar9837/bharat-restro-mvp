"""Entity resolution for deduplicating restaurant records."""

import uuid
from typing import Dict, List, Optional, Tuple

import geohash2
from rapidfuzz import fuzz

from .log import logger
from .persist import db_manager
from .utils import hash_content


def generate_entity_key(name: str, lat: Optional[float], lon: Optional[float]) -> str:
    """Generate entity key using trigram of name + geohash."""
    
    # Normalize name for key generation
    normalized_name = name.lower().strip()
    
    # Generate trigrams from name
    trigrams = set()
    for i in range(len(normalized_name) - 2):
        trigram = normalized_name[i:i+3]
        if trigram.isalnum() or ' ' in trigram:  # Keep alphanumeric trigrams
            trigrams.add(trigram)
    
    # Sort trigrams for consistency
    sorted_trigrams = sorted(trigrams)[:5]  # Take top 5 trigrams
    
    # Generate geohash if coordinates available
    geohash = ""
    if lat is not None and lon is not None:
        try:
            geohash = geohash2.encode(lat, lon, precision=6)  # ~1.2km precision
        except Exception as e:
            logger.warning("Failed to generate geohash", lat=lat, lon=lon, error=str(e))
    
    # Combine trigrams and geohash
    key_parts = sorted_trigrams + [geohash] if geohash else sorted_trigrams
    entity_key = "_".join(key_parts)
    
    return entity_key


def calculate_name_similarity(name1: str, name2: str) -> float:
    """Calculate name similarity using fuzzy matching."""
    if not name1 or not name2:
        return 0.0
    
    # Normalize names
    norm1 = name1.lower().strip()
    norm2 = name2.lower().strip()
    
    # Use rapidfuzz for similarity calculation
    similarity = fuzz.ratio(norm1, norm2) / 100.0
    
    return similarity


def calculate_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in kilometers."""
    from math import radians, sin, cos, sqrt, atan2
    
    # Haversine formula
    R = 6371.0  # Earth radius in km
    
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = sin(dlat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    distance = R * c
    return distance


def is_likely_same_restaurant(
    restaurant1: Dict[str, any], 
    restaurant2: Dict[str, any]
) -> Tuple[bool, float, Dict[str, any]]:
    """Determine if two restaurant records likely represent the same entity."""
    
    evidence = {
        "name_similarity": 0.0,
        "distance_km": float('inf'),
        "phone_match": False,
        "website_match": False,
        "address_similarity": 0.0,
        "overall_score": 0.0
    }
    
    # Name similarity
    name1 = restaurant1.get('canonical_name', '')
    name2 = restaurant2.get('canonical_name', '')
    
    if name1 and name2:
        evidence["name_similarity"] = calculate_name_similarity(name1, name2)
    
    # Distance calculation
    lat1, lon1 = restaurant1.get('lat'), restaurant1.get('lon')
    lat2, lon2 = restaurant2.get('lat'), restaurant2.get('lon')
    
    if all(coord is not None for coord in [lat1, lon1, lat2, lon2]):
        evidence["distance_km"] = calculate_distance_km(lat1, lon1, lat2, lon2)
    
    # Phone number match
    phone1 = restaurant1.get('phone', '')
    phone2 = restaurant2.get('phone', '')
    
    if phone1 and phone2:
        # Normalize phones for comparison
        phone1_digits = ''.join(filter(str.isdigit, phone1))
        phone2_digits = ''.join(filter(str.isdigit, phone2))
        
        # Check if last 10 digits match (for mobile numbers)
        if len(phone1_digits) >= 10 and len(phone2_digits) >= 10:
            evidence["phone_match"] = phone1_digits[-10:] == phone2_digits[-10:]
    
    # Website match
    website1 = restaurant1.get('website', '')
    website2 = restaurant2.get('website', '')
    
    if website1 and website2:
        # Normalize URLs for comparison
        norm_web1 = website1.lower().replace('http://', '').replace('https://', '').replace('www.', '')
        norm_web2 = website2.lower().replace('http://', '').replace('https://', '').replace('www.', '')
        evidence["website_match"] = norm_web1 == norm_web2
    
    # Address similarity
    addr1 = restaurant1.get('address_full', '')
    addr2 = restaurant2.get('address_full', '')
    
    if addr1 and addr2:
        evidence["address_similarity"] = calculate_name_similarity(addr1, addr2)
    
    # Calculate overall score
    score = 0.0
    
    # Name similarity (40% weight)
    score += evidence["name_similarity"] * 0.4
    
    # Distance penalty (30% weight)
    if evidence["distance_km"] < 0.5:  # Within 500m
        score += 0.3
    elif evidence["distance_km"] < 2.0:  # Within 2km
        score += 0.15
    
    # Phone match (20% weight)
    if evidence["phone_match"]:
        score += 0.2
    
    # Website match (10% weight)
    if evidence["website_match"]:
        score += 0.1
    
    # Address similarity bonus
    score += evidence["address_similarity"] * 0.1
    
    evidence["overall_score"] = min(score, 1.0)
    
    # Decision threshold
    is_same = evidence["overall_score"] > 0.7
    
    logger.debug(
        "Entity resolution comparison",
        name1=name1,
        name2=name2,
        score=evidence["overall_score"],
        is_same=is_same,
        evidence=evidence
    )
    
    return is_same, evidence["overall_score"], evidence


def find_existing_restaurant(new_restaurant: Dict[str, any]) -> Optional[str]:
    """Find existing restaurant that matches the new one."""
    
    name = new_restaurant.get('canonical_name', '')
    lat = new_restaurant.get('lat')
    lon = new_restaurant.get('lon')
    phone = new_restaurant.get('phone', '')
    
    if not name:
        logger.warning("Cannot resolve entity without name")
        return None
    
    logger.debug("Finding existing restaurant", name=name)
    
    # Search for potential matches
    # First, try exact name match
    exact_matches = db_manager.search_restaurants(name=name, limit=10)
    
    # Then try by city if we have location
    city_matches = []
    if lat and lon:
        # Extract city from address or use geocoding
        address = new_restaurant.get('address_full', '')
        if address:
            # Simple city extraction (could be improved)
            for city in ['bangalore', 'mumbai', 'delhi', 'chennai', 'hyderabad']:
                if city in address.lower():
                    city_matches = db_manager.search_restaurants(city=city, limit=20)
                    break
    
    # Finally, try phone number if available
    phone_matches = []
    if phone:
        # This would require a more sophisticated search
        # For now, we'll search all and filter by phone
        all_restaurants = db_manager.get_all_restaurants(limit=100)
        phone_matches = [r for r in all_restaurants if r.phone == phone]
    
    # Combine all candidates
    candidates = []
    candidates.extend(exact_matches)
    candidates.extend(city_matches)
    candidates.extend(phone_matches)
    
    # Remove duplicates
    seen_ids = set()
    unique_candidates = []
    for candidate in candidates:
        if candidate.restaurant_id not in seen_ids:
            unique_candidates.append(candidate)
            seen_ids.add(candidate.restaurant_id)
    
    logger.debug("Found potential candidates", count=len(unique_candidates))
    
    # Score each candidate
    best_match = None
    best_score = 0.0
    
    for candidate in unique_candidates:
        candidate_data = candidate.to_dict()
        
        is_same, score, evidence = is_likely_same_restaurant(new_restaurant, candidate_data)
        
        if is_same and score > best_score:
            best_match = candidate.restaurant_id
            best_score = score
            
            logger.debug(
                "Found potential match",
                candidate_id=candidate.restaurant_id,
                candidate_name=candidate.canonical_name,
                score=score
            )
    
    if best_match:
        logger.info("Found existing restaurant match", restaurant_id=best_match, score=best_score)
    else:
        logger.debug("No existing restaurant match found")
    
    return best_match


def resolve_restaurant_entity(restaurant_data: Dict[str, any]) -> str:
    """Resolve restaurant entity, returning existing ID or generating new one."""
    
    logger.debug("Resolving restaurant entity")
    
    # Try to find existing restaurant
    existing_id = find_existing_restaurant(restaurant_data)
    
    if existing_id:
        logger.info("Resolved to existing restaurant", restaurant_id=existing_id)
        return existing_id
    
    # Generate new restaurant ID
    new_id = str(uuid.uuid4())
    logger.info("Generated new restaurant ID", restaurant_id=new_id)
    
    return new_id


def merge_restaurant_data(
    existing_data: Dict[str, any], 
    new_data: Dict[str, any]
) -> Dict[str, any]:
    """Merge new restaurant data with existing data."""
    
    logger.debug("Merging restaurant data")
    
    merged = existing_data.copy()
    
    # Merge strategy: take new data if it has higher confidence or more complete information
    merge_fields = [
        'canonical_name', 'address_full', 'pincode', 'lat', 'lon', 
        'phone', 'website', 'cuisines', 'hours'
    ]
    
    for field in merge_fields:
        existing_value = existing_data.get(field)
        new_value = new_data.get(field)
        
        # If existing is empty/null, use new
        if not existing_value and new_value:
            merged[field] = new_value
            continue
        
        # If new is empty/null, keep existing
        if not new_value:
            continue
        
        # For lists (cuisines), merge unique values
        if field == 'cuisines' and isinstance(existing_value, list) and isinstance(new_value, list):
            merged_cuisines = list(set(existing_value + new_value))
            merged[field] = merged_cuisines
            continue
        
        # For other fields, prefer new data (assuming it's more recent)
        merged[field] = new_value
    
    logger.debug("Restaurant data merged")
    
    return merged
