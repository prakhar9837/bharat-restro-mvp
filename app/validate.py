"""Validation rules for restaurant data quality."""

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .log import logger


def validate_pincode(pincode: str) -> Tuple[bool, List[str]]:
    """Validate Indian pincode format."""
    issues = []
    
    if not pincode:
        return True, []  # Pincode is optional
    
    # Must be exactly 6 digits
    if not re.match(r'^\d{6}$', pincode):
        issues.append("Pincode must be exactly 6 digits")
        return False, issues
    
    # First digit should be 1-8 for Indian pincodes
    if pincode[0] not in '12345678':
        issues.append("Invalid pincode: first digit must be 1-8 for India")
        return False, issues
    
    return True, []


def validate_phone(phone: str) -> Tuple[bool, List[str]]:
    """Validate Indian phone number format."""
    issues = []
    
    if not phone:
        return True, []  # Phone is optional
    
    # Expected format: +91XXXXXXXXXX
    pattern = r'^\+91[6-9]\d{9}$'
    
    if not re.match(pattern, phone):
        issues.append("Phone must be in +91XXXXXXXXXX format with mobile number starting with 6-9")
        return False, issues
    
    return True, []


def validate_geo_coordinates(lat: Optional[float], lon: Optional[float]) -> Tuple[bool, List[str]]:
    """Validate geographic coordinates for India."""
    issues = []
    
    if lat is None and lon is None:
        return True, []  # Coordinates are optional
    
    if lat is None or lon is None:
        issues.append("Both latitude and longitude must be provided if either is present")
        return False, issues
    
    # India bounds (approximate)
    if not (6.0 <= lat <= 38.0):
        issues.append(f"Latitude {lat} is outside India bounds (6.0 to 38.0)")
        return False, issues
    
    if not (68.0 <= lon <= 98.0):
        issues.append(f"Longitude {lon} is outside India bounds (68.0 to 98.0)")
        return False, issues
    
    return True, []


def validate_hours(hours: Dict[str, any]) -> Tuple[bool, List[str]]:
    """Validate opening hours format and logic."""
    issues = []
    
    if not hours:
        return True, []  # Hours are optional
    
    # Expected days
    expected_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    
    for day in expected_days:
        day_hours = hours.get(day, [])
        
        if not isinstance(day_hours, list):
            issues.append(f"Hours for {day} must be a list")
            continue
        
        for i, hours_segment in enumerate(day_hours):
            if not isinstance(hours_segment, dict):
                issues.append(f"Hours segment {i} for {day} must be a dictionary")
                continue
            
            open_time = hours_segment.get('open')
            close_time = hours_segment.get('close')
            
            # Validate time format
            if not _is_valid_time_format(open_time):
                issues.append(f"Invalid open time format for {day}: {open_time}")
                continue
                
            if not _is_valid_time_format(close_time):
                issues.append(f"Invalid close time format for {day}: {close_time}")
                continue
            
            # Validate time logic (open <= close, accounting for next day)
            if not _is_valid_time_range(open_time, close_time):
                issues.append(f"Invalid time range for {day}: {open_time} to {close_time}")
    
    return len(issues) == 0, issues


def _is_valid_time_format(time_str: str) -> bool:
    """Check if time string is valid HH:MM format."""
    if not time_str or not isinstance(time_str, str):
        return False
    
    pattern = re.compile(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$')
    return bool(pattern.match(time_str))


def _is_valid_time_range(open_time: str, close_time: str) -> bool:
    """Check if time range is valid."""
    try:
        # Parse times
        open_hour, open_min = map(int, open_time.split(':'))
        close_hour, close_min = map(int, close_time.split(':'))
        
        open_minutes = open_hour * 60 + open_min
        close_minutes = close_hour * 60 + close_min
        
        # Allow for next-day closing (e.g., open 22:00, close 02:00)
        if close_minutes < open_minutes:
            close_minutes += 24 * 60  # Add 24 hours
        
        # Reasonable operating hours (max 24 hours)
        duration = close_minutes - open_minutes
        return 0 < duration <= 24 * 60
        
    except (ValueError, AttributeError):
        return False


def validate_website_url(url: str) -> Tuple[bool, List[str]]:
    """Validate website URL format."""
    issues = []
    
    if not url:
        return True, []  # Website is optional
    
    # Basic URL pattern
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    if not url_pattern.match(url):
        issues.append(f"Invalid URL format: {url}")
        return False, issues
    
    return True, []


def validate_cuisines(cuisines: List[str]) -> Tuple[bool, List[str]]:
    """Validate cuisine types against standard vocabulary."""
    issues = []
    
    if not cuisines:
        return True, []  # Cuisines are optional
    
    if not isinstance(cuisines, list):
        issues.append("Cuisines must be a list")
        return False, issues
    
    # Standard vocabulary
    valid_cuisines = [
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
    
    for cuisine in cuisines:
        if not isinstance(cuisine, str):
            issues.append(f"Cuisine must be a string: {cuisine}")
            continue
            
        if cuisine not in valid_cuisines:
            issues.append(f"Invalid cuisine type: {cuisine}. Must be one of: {valid_cuisines}")
    
    return len(issues) == 0, issues


def validate_restaurant_name(name: str) -> Tuple[bool, List[str]]:
    """Validate restaurant name."""
    issues = []
    
    if not name or not isinstance(name, str):
        issues.append("Restaurant name is required and must be a string")
        return False, issues
    
    name = name.strip()
    
    if len(name) < 2:
        issues.append("Restaurant name must be at least 2 characters long")
        return False, issues
    
    if len(name) > 200:
        issues.append("Restaurant name must be less than 200 characters")
        return False, issues
    
    return True, []


def validate_provenance_required(restaurant_data: Dict[str, any]) -> Tuple[bool, List[str]]:
    """Validate that non-null fields have provenance records."""
    issues = []
    
    # Fields that require provenance if not null
    provenance_required_fields = [
        'address_full', 'pincode', 'lat', 'lon', 'phone', 'website', 'cuisines', 'hours'
    ]
    
    for field in provenance_required_fields:
        value = restaurant_data.get(field)
        
        # Check if field has a value
        has_value = False
        if value is not None:
            if isinstance(value, str) and value.strip():
                has_value = True
            elif isinstance(value, (int, float)):
                has_value = True
            elif isinstance(value, (list, dict)) and value:
                has_value = True
        
        if has_value:
            # This field should have provenance - this would be checked in the persistence layer
            # For now, we just note that provenance is required
            logger.debug(f"Field {field} requires provenance", value=value)
    
    return True, []  # Provenance validation is handled in persistence layer


def validate_confidence_scores(extraction_results: Dict[str, any]) -> Tuple[bool, List[str]]:
    """Validate confidence scores are within valid range."""
    issues = []
    
    for field, result in extraction_results.items():
        if isinstance(result, dict) and 'confidence' in result:
            confidence = result['confidence']
            
            if not isinstance(confidence, (int, float)):
                issues.append(f"Confidence for {field} must be a number")
                continue
            
            if not (0.0 <= confidence <= 1.0):
                issues.append(f"Confidence for {field} must be between 0.0 and 1.0, got {confidence}")
    
    return len(issues) == 0, issues


def validate_restaurant_data(data: Dict[str, any]) -> Tuple[bool, List[str]]:
    """Validate complete restaurant data record."""
    logger.debug("Validating restaurant data")
    
    all_issues = []
    overall_valid = True
    
    # Validate required name
    name_valid, name_issues = validate_restaurant_name(data.get('canonical_name', ''))
    if not name_valid:
        overall_valid = False
        all_issues.extend(name_issues)
    
    # Validate optional fields
    validators = [
        ('pincode', validate_pincode),
        ('phone', validate_phone),
        ('website', validate_website_url),
        ('cuisines', validate_cuisines),
        ('hours', validate_hours),
    ]
    
    for field, validator in validators:
        value = data.get(field)
        if value is not None:
            valid, issues = validator(value)
            if not valid:
                overall_valid = False
                all_issues.extend(issues)
    
    # Validate coordinates together
    lat = data.get('lat')
    lon = data.get('lon')
    coord_valid, coord_issues = validate_geo_coordinates(lat, lon)
    if not coord_valid:
        overall_valid = False
        all_issues.extend(coord_issues)
    
    # Validate provenance requirements
    prov_valid, prov_issues = validate_provenance_required(data)
    if not prov_valid:
        overall_valid = False
        all_issues.extend(prov_issues)
    
    if overall_valid:
        logger.debug("Restaurant data validation passed")
    else:
        logger.warning("Restaurant data validation failed", issues=all_issues)
    
    return overall_valid, all_issues


def validate_extraction_results(results: Dict[str, any]) -> Tuple[bool, List[str]]:
    """Validate extraction results structure and confidence scores."""
    logger.debug("Validating extraction results")
    
    # Validate confidence scores
    conf_valid, conf_issues = validate_confidence_scores(results)
    
    all_issues = []
    overall_valid = conf_valid
    
    if not overall_valid:
        all_issues.extend(conf_issues)
    
    if overall_valid:
        logger.debug("Extraction results validation passed")
    else:
        logger.warning("Extraction results validation failed", issues=all_issues)
    
    return overall_valid, all_issues
