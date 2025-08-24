"""Router for directing chunks to appropriate extractors."""

from typing import Dict, List, Tuple

from . import address, phone, hours, cuisines
from ..log import logger
from ..parse import ContentChunk


class ExtractionRouter:
    """Route content chunks to appropriate extractors."""
    
    def __init__(self):
        self.extractors = {
            "address": address.extract_address,
            "phone": phone.extract_phone,
            "hours": hours.extract_hours,
            "cuisines": cuisines.extract_cuisines,
        }
    
    async def extract_all(self, chunks: List[ContentChunk]) -> Dict[str, any]:
        """Extract all information types from chunks."""
        
        logger.info("Starting extraction routing", chunks_count=len(chunks))
        
        # Convert chunks to text strings
        chunk_texts = [chunk.text for chunk in chunks]
        
        results = {}
        
        # Extract each information type
        for info_type, extractor in self.extractors.items():
            try:
                logger.debug(f"Extracting {info_type}")
                
                if info_type == "cuisines":
                    # Cuisines extractor returns List[str] instead of single value
                    value, confidence, used_chunks = await extractor(chunk_texts)
                else:
                    value, confidence, used_chunks = await extractor(chunk_texts)
                
                results[info_type] = {
                    "value": value,
                    "confidence": confidence,
                    "used_chunks": used_chunks,
                    "source_chunks": [chunks[i].to_dict() for i in used_chunks if i < len(chunks)]
                }
                
                logger.debug(
                    f"Extracted {info_type}",
                    confidence=confidence,
                    used_chunks_count=len(used_chunks)
                )
                
            except Exception as e:
                logger.error(f"Failed to extract {info_type}", error=str(e))
                results[info_type] = {
                    "value": None,
                    "confidence": 0.0,
                    "used_chunks": [],
                    "source_chunks": [],
                    "error": str(e)
                }
        
        logger.info("Extraction routing completed", extracted_types=list(results.keys()))
        
        return results
    
    def get_relevant_chunks(self, chunks: List[ContentChunk], info_type: str) -> List[ContentChunk]:
        """Get chunks most relevant for a specific information type."""
        
        # Keywords for different information types
        relevance_keywords = {
            "address": ["address", "location", "find", "reach", "visit", "directions"],
            "phone": ["phone", "call", "contact", "mobile", "telephone"],
            "hours": ["hours", "open", "close", "timing", "schedule", "time"],
            "cuisines": ["cuisine", "food", "menu", "serves", "speciality", "dishes"],
        }
        
        keywords = relevance_keywords.get(info_type, [])
        if not keywords:
            return chunks
        
        # Score chunks by relevance
        scored_chunks = []
        for chunk in chunks:
            score = 0
            text_lower = chunk.text.lower()
            
            # Count keyword matches
            for keyword in keywords:
                score += text_lower.count(keyword)
            
            # Boost score for title chunks
            if chunk.chunk_type == "title":
                score += 1
            
            # Boost score for structured data
            if chunk.chunk_type == "structured_data":
                score += 2
            
            scored_chunks.append((score, chunk))
        
        # Sort by score and return top chunks
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        
        # Return top 5 most relevant chunks
        return [chunk for score, chunk in scored_chunks[:5]]


# Global router instance
extraction_router = ExtractionRouter()


async def route_and_extract(chunks: List[ContentChunk]) -> Dict[str, any]:
    """Route chunks and extract all information types."""
    return await extraction_router.extract_all(chunks)
