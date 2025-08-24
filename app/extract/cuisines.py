"""Cuisine extraction with vocabulary mapping and LLM enhancement."""

import re
from typing import Dict, List, Tuple

from .llm_client import LLMDisabled, generate_json, ollama_client
from ..log import logger


class CuisineExtractor:
    """Extract and map cuisine types to standard vocabulary."""
    
    def __init__(self):
        # Standard cuisine vocabulary
        self.standard_cuisines = [
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
        
        # Cuisine mapping patterns
        self.cuisine_patterns = {
            "NORTH_INDIAN": [
                r'north indian', r'punjabi', r'rajasthani', r'delhi', r'hindi',
                r'naan', r'roti', r'tandoor', r'curry', r'dal', r'paneer'
            ],
            "SOUTH_INDIAN": [
                r'south indian', r'tamil', r'kerala', r'andhra', r'karnataka',
                r'dosa', r'idli', r'sambar', r'rasam', r'vada', r'uttapam',
                r'biryani', r'chettinad', r'malabari'
            ],
            "CHINESE": [
                r'chinese', r'indo-chinese', r'chow mein', r'fried rice',
                r'manchurian', r'hakka', r'szechuan', r'canton'
            ],
            "STREET_FOOD": [
                r'street food', r'chaat', r'pani puri', r'bhel', r'pav bhaji',
                r'vada pav', r'samosa', r'kachori', r'fast food', r'snacks'
            ],
            "BAKERY": [
                r'bakery', r'bread', r'cake', r'pastry', r'cookies', r'biscuits',
                r'baked', r'oven', r'croissant', r'muffin'
            ],
            "CAFE": [
                r'cafe', r'coffee', r'tea', r'beverages', r'espresso', r'latte',
                r'cappuccino', r'sandwich', r'light meal'
            ],
            "ITALIAN": [
                r'italian', r'pizza', r'pasta', r'spaghetti', r'lasagna',
                r'risotto', r'gelato', r'mediterranean'
            ],
            "MUGHLAI": [
                r'mughlai', r'mughal', r'kebab', r'biryani', r'korma',
                r'nawabi', r'lucknowi', r'awadhi', r'dum'
            ],
            "SEAFOOD": [
                r'seafood', r'fish', r'prawn', r'crab', r'lobster', r'marine',
                r'coastal', r'goan', r'mangalorean', r'konkani'
            ]
        }
        
        # Compile patterns for efficiency
        self.compiled_patterns = {}
        for cuisine, patterns in self.cuisine_patterns.items():
            self.compiled_patterns[cuisine] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
    
    async def extract(self, chunks: List[str]) -> Tuple[List[str], float, List[int]]:
        """Extract cuisine types from text chunks."""
        
        logger.debug("Starting cuisine extraction", chunks_count=len(chunks))
        
        # Try regex-based extraction first
        regex_cuisines, regex_confidence, used_chunks = self._extract_with_regex(chunks)
        
        # If confidence is low, try LLM
        if regex_confidence < 0.6:
            try:
                llm_cuisines, llm_confidence, llm_chunks = await self._extract_with_llm(chunks)
                
                if llm_confidence > regex_confidence:
                    logger.debug("Using LLM result for cuisines", confidence=llm_confidence)
                    return llm_cuisines, llm_confidence, llm_chunks
                    
            except LLMDisabled:
                logger.debug("LLM disabled, using regex result for cuisines")
            except Exception as e:
                logger.warning("LLM extraction failed for cuisines", error=str(e))
        
        logger.debug("Using regex result for cuisines", confidence=regex_confidence)
        return regex_cuisines, regex_confidence, used_chunks
    
    def _extract_with_regex(self, chunks: List[str]) -> Tuple[List[str], float, List[int]]:
        """Extract cuisines using regex patterns."""
        
        found_cuisines = set()
        used_chunks = []
        confidence_factors = []
        
        # Combine all chunks into searchable text
        combined_text = " ".join(chunks).lower()
        
        # Search for cuisine patterns
        for cuisine, patterns in self.compiled_patterns.items():
            for pattern in patterns:
                matches = pattern.findall(combined_text)
                if matches:
                    found_cuisines.add(cuisine)
                    
                    # Find which chunks contained the matches
                    for i, chunk in enumerate(chunks):
                        if pattern.search(chunk.lower()):
                            if i not in used_chunks:
                                used_chunks.append(i)
                    
                    # Add confidence based on match strength
                    if len(matches) > 1:
                        confidence_factors.append(0.2)  # Multiple matches
                    else:
                        confidence_factors.append(0.15)  # Single match
                    
                    break  # Found this cuisine, move to next
        
        # Also look for explicit cuisine mentions
        cuisine_indicators = [
            r'cuisine', r'food', r'speciality', r'specialty', r'serves',
            r'menu', r'dishes', r'kitchen', r'cooking'
        ]
        
        for i, chunk in enumerate(chunks):
            chunk_lower = chunk.lower()
            
            for indicator in cuisine_indicators:
                if re.search(indicator, chunk_lower):
                    # Look for cuisine names near indicators
                    for cuisine in self.standard_cuisines:
                        cuisine_words = cuisine.lower().replace('_', ' ')
                        if cuisine_words in chunk_lower:
                            found_cuisines.add(cuisine)
                            if i not in used_chunks:
                                used_chunks.append(i)
                            confidence_factors.append(0.1)
        
        # Calculate overall confidence
        cuisines_list = list(found_cuisines)
        confidence = 0.0
        
        if cuisines_list:
            base_confidence = min(sum(confidence_factors), 0.8)
            
            # Boost confidence if multiple cuisines found (more comprehensive)
            if len(cuisines_list) > 1:
                base_confidence += 0.1
            
            confidence = min(base_confidence, 1.0)
        
        return cuisines_list, confidence, used_chunks
    
    async def _extract_with_llm(self, chunks: List[str]) -> Tuple[List[str], float, List[int]]:
        """Extract cuisines using LLM."""
        
        # Combine relevant chunks
        text = "\n".join(chunks[:3])
        
        schema = ollama_client.get_schema("cuisines")
        if not schema:
            raise ValueError("Cuisines schema not loaded")
        
        user_prompt = f"""Extract the cuisine types for this restaurant from the text:

{text}

Map to standard cuisine categories: {', '.join(self.standard_cuisines)}

Focus on the type of food served, cooking style, and cultural origin."""
        
        result = await generate_json(
            user_prompt=user_prompt,
            schema=schema,
            temperature=0.1
        )
        
        cuisines = result.get("cuisines", [])
        confidence = result.get("confidence", 0.0)
        used_chunks = list(range(min(len(chunks), 3)))
        
        # Validate cuisines are in standard vocabulary
        valid_cuisines = [c for c in cuisines if c in self.standard_cuisines]
        
        # Reduce confidence if some cuisines were invalid
        if len(valid_cuisines) != len(cuisines):
            confidence *= 0.8
        
        return valid_cuisines, confidence, used_chunks
    
    def map_cuisine_text(self, text: str) -> List[str]:
        """Map arbitrary cuisine text to standard vocabulary."""
        text_lower = text.lower()
        mapped_cuisines = []
        
        for cuisine, patterns in self.compiled_patterns.items():
            for pattern in patterns:
                if pattern.search(text_lower):
                    mapped_cuisines.append(cuisine)
                    break
        
        return list(set(mapped_cuisines))  # Remove duplicates


# Global extractor instance
cuisine_extractor = CuisineExtractor()


async def extract_cuisines(chunks: List[str]) -> Tuple[List[str], float, List[int]]:
    """Extract cuisine types from text chunks."""
    return await cuisine_extractor.extract(chunks)


def map_cuisine_text(text: str) -> List[str]:
    """Map cuisine text to standard vocabulary."""
    return cuisine_extractor.map_cuisine_text(text)
