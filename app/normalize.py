"""Data normalization utilities for Indian restaurant data."""

import re
from typing import Dict, List, Optional

from .log import logger


def normalize_phone(phone: str) -> Optional[str]:
    """Normalize phone number to +91XXXXXXXXXX format."""
    if not phone:
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'[^\d]', '', phone)
    
    # Handle different formats
    if len(digits) == 10 and digits[0] in '6789':
        # 10-digit mobile number
        return f"+91{digits}"
    elif len(digits) == 12 and digits.startswith('91') and digits[2] in '6789':
        # 91XXXXXXXXXX format
        return f"+{digits}"
    elif len(digits) == 13 and digits.startswith('91') and digits[3] in '6789':
        # 91XXXXXXXXXXX (extra digit, extract 10)
        return f"+91{digits[3:]}"
    elif len(digits) >= 10:
        # Try to find 10-digit mobile within the string
        for i in range(len(digits) - 9):
            candidate = digits[i:i+10]
            if candidate[0] in '6789':
                return f"+91{candidate}"
    
    # If can't normalize to mobile, return as-is if it looks like a landline
    if len(digits) >= 10:
        return phone.strip()
    
    return None


def normalize_pincode(pincode: str) -> Optional[str]:
    """Normalize pincode to 6-digit format."""
    if not pincode:
        return None
    
    # Extract only digits
    digits = re.sub(r'[^\d]', '', pincode)
    
    # Must be exactly 6 digits for Indian pincode
    if len(digits) == 6:
        return digits
    
    return None


def normalize_address_fields(address_data: Dict[str, any]) -> Dict[str, any]:
    """Normalize address field values."""
    if not address_data:
        return {}
    
    normalized = {}
    
    # String fields to clean
    string_fields = ["full", "house_no", "street", "locality", "city", "state"]
    
    for field in string_fields:
        value = address_data.get(field)
        if value and isinstance(value, str):
            # Clean whitespace and normalize
            cleaned = value.strip()
            if cleaned:
                # Capitalize words properly
                if field in ["city", "state", "locality"]:
                    cleaned = cleaned.title()
                normalized[field] = cleaned
        else:
            normalized[field] = value
    
    # Special handling for pincode
    pincode = address_data.get("pincode")
    normalized["pincode"] = normalize_pincode(pincode) if pincode else None
    
    # Preserve confidence
    normalized["confidence"] = address_data.get("confidence", 0.0)
    
    return normalized


def map_cuisines_to_vocab(cuisines: List[str]) -> List[str]:
    """Map cuisine names to standard vocabulary."""
    if not cuisines:
        return []
    
    # Standard vocabulary
    standard_vocab = [
        "NORTH_INDIAN",
        "SOUTH_INDIAN", 
        "CHINESE",
        "STREET_FOOD",
        "BAKERY",
        "CAFE",
        "ITALIAN",
        "MUGHLAI",
        "SEAFOOD"
    ]
    
    # Mapping rules
    cuisine_mapping = {
        # North Indian variations
        "north indian": "NORTH_INDIAN",
        "punjabi": "NORTH_INDIAN",
        "rajasthani": "NORTH_INDIAN",
        "hindi": "NORTH_INDIAN",
        "tandoor": "NORTH_INDIAN",
        
        # South Indian variations
        "south indian": "SOUTH_INDIAN",
        "tamil": "SOUTH_INDIAN",
        "kerala": "SOUTH_INDIAN",
        "andhra": "SOUTH_INDIAN",
        "karnataka": "SOUTH_INDIAN",
        "chettinad": "SOUTH_INDIAN",
        "malabari": "SOUTH_INDIAN",
        
        # Chinese variations
        "chinese": "CHINESE",
        "indo-chinese": "CHINESE",
        "indo chinese": "CHINESE",
        "hakka": "CHINESE",
        "szechuan": "CHINESE",
        "canton": "CHINESE",
        
        # Street food variations
        "street food": "STREET_FOOD",
        "chaat": "STREET_FOOD",
        "fast food": "STREET_FOOD",
        "snacks": "STREET_FOOD",
        
        # Bakery variations
        "bakery": "BAKERY",
        "baked": "BAKERY",
        "pastry": "BAKERY",
        
        # Cafe variations
        "cafe": "CAFE",
        "coffee": "CAFE",
        "tea": "CAFE",
        "beverages": "CAFE",
        
        # Italian variations
        "italian": "ITALIAN",
        "pizza": "ITALIAN",
        "pasta": "ITALIAN",
        "mediterranean": "ITALIAN",
        
        # Mughlai variations
        "mughlai": "MUGHLAI",
        "mughal": "MUGHLAI",
        "nawabi": "MUGHLAI",
        "lucknowi": "MUGHLAI",
        "awadhi": "MUGHLAI",
        
        # Seafood variations
        "seafood": "SEAFOOD",
        "fish": "SEAFOOD",
        "marine": "SEAFOOD",
        "coastal": "SEAFOOD",
        "goan": "SEAFOOD",
        "mangalorean": "SEAFOOD",
        "konkani": "SEAFOOD",
    }
    
    mapped_cuisines = set()
    
    for cuisine in cuisines:
        if not cuisine:
            continue
            
        # Check if already in standard format
        if cuisine.upper() in standard_vocab:
            mapped_cuisines.add(cuisine.upper())
            continue
        
        # Try to map from variations
        cuisine_lower = cuisine.lower().strip()
        
        # Direct mapping
        if cuisine_lower in cuisine_mapping:
            mapped_cuisines.add(cuisine_mapping[cuisine_lower])
            continue
        
        # Partial matching for compound terms
        for variant, standard in cuisine_mapping.items():
            if variant in cuisine_lower or cuisine_lower in variant:
                mapped_cuisines.add(standard)
                break
    
    return list(mapped_cuisines)


def normalize_restaurant_name(name: str) -> str:
    """Normalize restaurant name for consistency."""
    if not name:
        return ""
    
    # Basic cleaning
    normalized = name.strip()
    
    # Remove common prefixes/suffixes that don't add value
    prefixes_to_remove = ["the ", "hotel ", "new "]
    suffixes_to_remove = [" restaurant", " hotel", " dhaba"]
    
    name_lower = normalized.lower()
    
    # Remove prefixes
    for prefix in prefixes_to_remove:
        if name_lower.startswith(prefix):
            normalized = normalized[len(prefix):]
            break
    
    # Remove suffixes  
    for suffix in suffixes_to_remove:
        if name_lower.endswith(suffix):
            normalized = normalized[:-len(suffix)]
            break
    
    # Clean up spacing and capitalization
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Title case
    normalized = normalized.title()
    
    return normalized


def normalize_website_url(url: str) -> Optional[str]:
    """Normalize website URL."""
    if not url:
        return None
    
    url = url.strip()
    
    # Add protocol if missing
    if url and not url.startswith(('http://', 'https://')):
        url = f"https://{url}"
    
    # Basic validation
    if len(url) < 10 or '.' not in url:
        return None
    
    return url


def transliterate_indian_text(text: str) -> str:
    """Basic transliteration support for Indian languages."""
    # This is a simplified version - in production you'd use a proper transliteration library
    
    # Common Hindi/Indian language terms to English
    transliterations = {
        'रेस्तराँ': 'restaurant',
        'होटल': 'hotel',
        'कैफे': 'cafe',
        'खाना': 'food',
        'भोजन': 'food',
        'स्वादिष्ट': 'delicious',
        'ढाबा': 'dhaba',
    }
    
    for hindi, english in transliterations.items():
        text = text.replace(hindi, english)
    
    return text


def normalize_hours_format(hours_data: Dict[str, any]) -> Dict[str, any]:
    """Normalize hours data format."""
    if not hours_data:
        return {}
    
    normalized = {}
    
    # Expected days
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    
    for day in days:
        day_hours = hours_data.get(day, [])
        
        if not day_hours:
            normalized[day] = []
            continue
        
        # Ensure it's a list
        if not isinstance(day_hours, list):
            day_hours = [day_hours]
        
        normalized_day_hours = []
        
        for hours_segment in day_hours:
            if isinstance(hours_segment, dict):
                open_time = hours_segment.get('open')
                close_time = hours_segment.get('close')
                
                # Validate time format (HH:MM)
                if _is_valid_time(open_time) and _is_valid_time(close_time):
                    normalized_day_hours.append({
                        'open': open_time,
                        'close': close_time
                    })
        
        normalized[day] = normalized_day_hours
    
    # Preserve confidence
    normalized['confidence'] = hours_data.get('confidence', 0.0)
    
    return normalized


def _is_valid_time(time_str: str) -> bool:
    """Check if time string is valid HH:MM format."""
    if not time_str or not isinstance(time_str, str):
        return False
    
    pattern = re.compile(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$')
    return bool(pattern.match(time_str))


# Main normalization function
def normalize_restaurant_data(data: Dict[str, any]) -> Dict[str, any]:
    """Normalize all restaurant data fields."""
    logger.debug("Normalizing restaurant data")
    
    normalized = {}
    
    # Name - check both 'name' and 'canonical_name' fields
    name_field = data.get('canonical_name') or data.get('name')
    if name_field:
        normalized['canonical_name'] = normalize_restaurant_name(name_field)
    
    # Address - handle multiple field names
    address_field = data.get('address_full') or data.get('address')
    if address_field:
        if isinstance(address_field, dict):
            normalized['address_full'] = address_field.get('full')
            if 'pincode' in address_field:
                normalized['pincode'] = normalize_pincode(address_field['pincode'])
        else:
            normalized['address_full'] = address_field
    
    # Also check for separate pincode field
    if 'pincode' in data and 'pincode' not in normalized:
        normalized['pincode'] = normalize_pincode(data['pincode'])
    
    # Phone
    if 'phone' in data:
        normalized['phone'] = normalize_phone(data['phone'])
    
    # Website
    if 'website' in data:
        normalized['website'] = normalize_website_url(data['website'])
    
    # Cuisines
    if 'cuisines' in data:
        normalized['cuisines'] = map_cuisines_to_vocab(data['cuisines'])
    
    # Hours
    if 'hours' in data:
        normalized['hours'] = normalize_hours_format(data['hours'])
    
    # Coordinates (already normalized)
    for field in ['lat', 'lon']:
        if field in data:
            normalized[field] = data[field]
    
    logger.debug("Data normalization completed")
    return normalized
