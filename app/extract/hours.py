"""Hours extraction with regex patterns and LLM enhancement."""

import re
from typing import Dict, List, Optional, Tuple

from .llm_client import LLMDisabled, generate_json, ollama_client
from ..log import logger


class HoursExtractor:
    """Extract opening hours from text chunks."""
    
    def __init__(self):
        # Day patterns
        self.day_patterns = {
            'monday': r'mon(?:day)?',
            'tuesday': r'tue(?:s(?:day)?)?',
            'wednesday': r'wed(?:nesday)?',
            'thursday': r'thu(?:r(?:s(?:day)?)?)?',
            'friday': r'fri(?:day)?',
            'saturday': r'sat(?:urday)?',
            'sunday': r'sun(?:day)?'
        }
        
        # Time patterns (24-hour and 12-hour)
        self.time_pattern = re.compile(
            r'(\d{1,2}):?(\d{2})?\s*(am|pm|AM|PM)?'
        )
        
        # Hours indicators
        self.hours_indicators = [
            r'hours?', r'timing', r'open', r'close', r'closed', r'available',
            r'schedule', r'time', r'समय', r'खुला', r'बंद'
        ]
    
    async def extract(self, chunks: List[str]) -> Tuple[Dict[str, any], float, List[int]]:
        """Extract opening hours from text chunks."""
        
        logger.debug("Starting hours extraction", chunks_count=len(chunks))
        
        # Try regex extraction first
        regex_result, regex_confidence, used_chunks = self._extract_with_regex(chunks)
        
        # If confidence is low, try LLM
        if regex_confidence < 0.6:
            try:
                llm_result, llm_confidence, llm_chunks = await self._extract_with_llm(chunks)
                
                if llm_confidence > regex_confidence:
                    logger.debug("Using LLM result for hours", confidence=llm_confidence)
                    return llm_result, llm_confidence, llm_chunks
                    
            except LLMDisabled:
                logger.debug("LLM disabled, using regex result for hours")
            except Exception as e:
                logger.warning("LLM extraction failed for hours", error=str(e))
        
        logger.debug("Using regex result for hours", confidence=regex_confidence)
        return regex_result, regex_confidence, used_chunks
    
    def _extract_with_regex(self, chunks: List[str]) -> Tuple[Dict[str, any], float, List[int]]:
        """Extract hours using regex patterns."""
        
        result = {day: [] for day in self.day_patterns.keys()}
        result["confidence"] = 0.0
        
        used_chunks = []
        confidence_factors = []
        
        # Find chunks with hours information
        for i, chunk in enumerate(chunks):
            chunk_lower = chunk.lower()
            
            # Check if chunk contains hours indicators
            has_hours_indicator = any(
                re.search(indicator, chunk_lower) for indicator in self.hours_indicators
            )
            
            if has_hours_indicator:
                used_chunks.append(i)
                
                # Try to extract hours for each day
                day_hours = self._extract_day_hours(chunk)
                if day_hours:
                    for day, hours_list in day_hours.items():
                        if hours_list:
                            result[day] = hours_list
                            confidence_factors.append(0.15)
                
                # Try to extract general hours (applied to all days)
                general_hours = self._extract_general_hours(chunk)
                if general_hours and not any(result.values()):
                    # Apply to all days if no specific hours found
                    for day in result.keys():
                        if day != "confidence":
                            result[day] = [general_hours]
                    confidence_factors.append(0.1)
        
        # Calculate confidence
        if used_chunks and confidence_factors:
            result["confidence"] = min(sum(confidence_factors), 0.8)
        
        return result, result["confidence"], used_chunks
    
    def _extract_day_hours(self, text: str) -> Dict[str, List[Dict[str, str]]]:
        """Extract hours for specific days."""
        day_hours = {}
        
        for day, pattern in self.day_patterns.items():
            # Look for day followed by hours
            day_regex = re.compile(
                f'{pattern}[:\-\s]*([^\n]*?)(?=(?:mon|tue|wed|thu|fri|sat|sun)|$)',
                re.IGNORECASE
            )
            
            match = day_regex.search(text)
            if match:
                hours_text = match.group(1).strip()
                hours = self._parse_hours_text(hours_text)
                if hours:
                    day_hours[day] = hours
        
        return day_hours
    
    def _extract_general_hours(self, text: str) -> Optional[Dict[str, str]]:
        """Extract general hours that apply to all days."""
        
        # Look for patterns like "9:00 AM - 10:00 PM" or "9-22"
        patterns = [
            r'(\d{1,2}):?(\d{2})?\s*(am|pm)?\s*[-–—to]\s*(\d{1,2}):?(\d{2})?\s*(am|pm)?',
            r'(\d{1,2})\s*[-–—to]\s*(\d{1,2})',
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                # Extract and normalize times
                open_time = self._normalize_time(match.groups()[:3])
                close_time = self._normalize_time(match.groups()[3:])
                
                if open_time and close_time:
                    return {"open": open_time, "close": close_time}
        
        return None
    
    def _parse_hours_text(self, hours_text: str) -> List[Dict[str, str]]:
        """Parse hours from text segment."""
        hours_list = []
        
        # Handle "closed" case
        if re.search(r'closed|close|holiday|बंद', hours_text, re.IGNORECASE):
            return []
        
        # Extract time ranges
        time_ranges = re.finditer(
            r'(\d{1,2}):?(\d{2})?\s*(am|pm)?\s*[-–—to]\s*(\d{1,2}):?(\d{2})?\s*(am|pm)?',
            hours_text,
            re.IGNORECASE
        )
        
        for match in time_ranges:
            open_time = self._normalize_time(match.groups()[:3])
            close_time = self._normalize_time(match.groups()[3:])
            
            if open_time and close_time:
                hours_list.append({"open": open_time, "close": close_time})
        
        return hours_list
    
    def _normalize_time(self, time_parts: tuple) -> Optional[str]:
        """Normalize time to HH:MM format."""
        try:
            hour_str, minute_str, period = time_parts
            
            if not hour_str:
                return None
            
            hour = int(hour_str)
            minute = int(minute_str) if minute_str else 0
            
            # Handle 12-hour format
            if period and period.upper() in ['AM', 'PM']:
                if period.upper() == 'PM' and hour != 12:
                    hour += 12
                elif period.upper() == 'AM' and hour == 12:
                    hour = 0
            
            # Validate hour and minute
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return f"{hour:02d}:{minute:02d}"
            
        except (ValueError, TypeError):
            pass
        
        return None
    
    async def _extract_with_llm(self, chunks: List[str]) -> Tuple[Dict[str, any], float, List[int]]:
        """Extract hours using LLM."""
        
        # Combine relevant chunks
        text = "\n".join(chunks[:3])
        
        schema = ollama_client.get_schema("hours")
        if not schema:
            raise ValueError("Hours schema not loaded")
        
        user_prompt = f"""Extract the restaurant opening hours from this text:

{text}

Look for daily schedules, opening and closing times. If the restaurant is closed on certain days, leave those days empty."""
        
        result = await generate_json(
            user_prompt=user_prompt,
            schema=schema,
            temperature=0.1
        )
        
        confidence = result.get("confidence", 0.0)
        used_chunks = list(range(min(len(chunks), 3)))
        
        return result, confidence, used_chunks


# Global extractor instance
hours_extractor = HoursExtractor()


async def extract_hours(chunks: List[str]) -> Tuple[Dict[str, any], float, List[int]]:
    """Extract opening hours from text chunks."""
    return await hours_extractor.extract(chunks)
