"""Address extraction with regex fallback and LLM enhancement."""

import re
from typing import Dict, List, Optional, Tuple

from .llm_client import LLMDisabled, generate_json, ollama_client
from ..log import logger


class AddressExtractor:
    """Extract and normalize address information from text chunks."""
    
    def __init__(self):
        # Indian address patterns
        self.pincode_pattern = re.compile(r'\b\d{6}\b')
        self.address_indicators = [
            r'address', r'location', r'situated', r'located', r'find us',
            r'reach us', r'visit us', r'directions', r'गता', r'पता'
        ]
        
        # Common Indian address components
        self.road_keywords = ['road', 'rd', 'street', 'st', 'lane', 'marg', 'path']
        self.area_keywords = ['nagar', 'colony', 'extension', 'sector', 'block', 'phase']
        
    async def extract(self, chunks: List[str]) -> Tuple[Dict[str, any], float, List[int]]:
        """Extract address information from text chunks."""
        
        logger.debug("Starting address extraction", chunks_count=len(chunks))
        
        # First, try regex-based extraction
        regex_result, regex_confidence, used_chunks = self._extract_with_regex(chunks)
        
        # If LLM is enabled, try to enhance with LLM
        if regex_confidence < 0.7:
            try:
                llm_result, llm_confidence, llm_chunks = await self._extract_with_llm(chunks)
                
                # Use LLM result if it's more confident
                if llm_confidence > regex_confidence:
                    logger.debug("Using LLM result for address", confidence=llm_confidence)
                    return llm_result, llm_confidence, llm_chunks
                    
            except LLMDisabled:
                logger.debug("LLM disabled, using regex result for address")
            except Exception as e:
                logger.warning("LLM extraction failed for address", error=str(e))
        
        logger.debug("Using regex result for address", confidence=regex_confidence)
        return regex_result, regex_confidence, used_chunks
    
    def _extract_with_regex(self, chunks: List[str]) -> Tuple[Dict[str, any], float, List[int]]:
        """Extract address using regex patterns."""
        
        result = {
            "full": None,
            "house_no": None,
            "street": None,
            "locality": None,
            "city": None,
            "state": None,
            "pincode": None,
            "confidence": 0.0
        }
        
        used_chunks = []
        confidence_factors = []
        
        # Search for address-like content
        for i, chunk in enumerate(chunks):
            chunk_lower = chunk.lower()
            
            # Check if chunk contains address indicators
            has_address_indicator = any(
                re.search(indicator, chunk_lower) for indicator in self.address_indicators
            )
            
            if has_address_indicator:
                used_chunks.append(i)
                
                # Extract pincode
                pincode_matches = self.pincode_pattern.findall(chunk)
                if pincode_matches:
                    result["pincode"] = pincode_matches[0]
                    confidence_factors.append(0.3)
                
                # Extract full address (heuristic)
                lines = chunk.split('\n')
                for line in lines:
                    line = line.strip()
                    if len(line) > 20 and any(keyword in line.lower() for keyword in self.road_keywords + self.area_keywords):
                        if not result["full"] or len(line) > len(result["full"]):
                            result["full"] = line
                            confidence_factors.append(0.2)
                
                # Try to extract city and state
                city_state = self._extract_city_state(chunk)
                if city_state:
                    result.update(city_state)
                    confidence_factors.append(0.15)
        
        # Calculate overall confidence
        if used_chunks:
            base_confidence = min(sum(confidence_factors), 0.8)
            
            # Boost confidence if we have pincode
            if result["pincode"]:
                base_confidence += 0.1
            
            # Boost confidence if we have full address
            if result["full"] and len(result["full"]) > 30:
                base_confidence += 0.1
            
            result["confidence"] = min(base_confidence, 1.0)
        
        return result, result["confidence"], used_chunks
    
    async def _extract_with_llm(self, chunks: List[str]) -> Tuple[Dict[str, any], float, List[int]]:
        """Extract address using LLM."""
        
        # Combine relevant chunks
        text = "\n".join(chunks[:3])  # Use first 3 chunks to avoid context limit
        
        schema = ollama_client.get_schema("address")
        if not schema:
            raise ValueError("Address schema not loaded")
        
        user_prompt = f"""Extract the restaurant address from this text:

{text}

Focus on finding the complete address including street, area, city, state, and pincode."""
        
        result = await generate_json(
            user_prompt=user_prompt,
            schema=schema,
            temperature=0.1
        )
        
        confidence = result.get("confidence", 0.0)
        used_chunks = list(range(min(len(chunks), 3)))  # Used first 3 chunks
        
        return result, confidence, used_chunks
    
    def _extract_city_state(self, text: str) -> Dict[str, Optional[str]]:
        """Extract city and state using patterns."""
        
        # Common Indian cities
        cities = [
            'bangalore', 'bengaluru', 'mumbai', 'delhi', 'chennai', 'kolkata',
            'hyderabad', 'pune', 'ahmedabad', 'jaipur', 'lucknow', 'kanpur',
            'nagpur', 'indore', 'thane', 'bhopal', 'visakhapatnam', 'pimpri'
        ]
        
        # Indian states
        states = [
            'karnataka', 'maharashtra', 'delhi', 'tamil nadu', 'west bengal',
            'telangana', 'gujarat', 'rajasthan', 'uttar pradesh', 'madhya pradesh',
            'andhra pradesh', 'kerala', 'punjab', 'haryana', 'bihar', 'odisha'
        ]
        
        text_lower = text.lower()
        
        result = {"city": None, "state": None}
        
        # Find city
        for city in cities:
            if city in text_lower:
                result["city"] = city.title()
                break
        
        # Find state
        for state in states:
            if state in text_lower:
                result["state"] = state.title()
                break
        
        return result


# Global extractor instance
address_extractor = AddressExtractor()


async def extract_address(chunks: List[str]) -> Tuple[Dict[str, any], float, List[int]]:
    """Extract address from text chunks."""
    return await address_extractor.extract(chunks)
