"""Phone number extraction with regex-first approach and LLM disambiguation."""

import re
from typing import List, Optional, Tuple

from .llm_client import LLMDisabled, generate_json
from ..log import logger
from ..utils import parse_phone_variants


class PhoneExtractor:
    """Extract and normalize phone numbers from text chunks."""
    
    def __init__(self):
        # Indian phone number patterns
        self.patterns = [
            # +91 formats
            re.compile(r'\+91[-\s]?[6-9]\d{9}'),
            re.compile(r'\+91[-\s]?\d{2,4}[-\s]?\d{6,8}'),  # Landline
            
            # 91 format without +
            re.compile(r'91[-\s]?[6-9]\d{9}'),
            re.compile(r'91[-\s]?\d{2,4}[-\s]?\d{6,8}'),
            
            # 10-digit mobile
            re.compile(r'[6-9]\d{9}'),
            
            # Landline with STD code
            re.compile(r'0\d{2,4}[-\s]?\d{6,8}'),
        ]
        
        # Phone indicators
        self.phone_indicators = [
            r'phone', r'mobile', r'call', r'contact', r'tel', r'telephone',
            r'mob', r'cell', r'फोन', r'मोबाइल'
        ]
    
    async def extract(self, chunks: List[str]) -> Tuple[Optional[str], float, List[int]]:
        """Extract phone number from text chunks."""
        
        logger.debug("Starting phone extraction", chunks_count=len(chunks))
        
        # Extract all potential phone numbers
        candidates = []
        used_chunks = []
        
        for i, chunk in enumerate(chunks):
            chunk_phones = self._extract_phones_from_chunk(chunk)
            if chunk_phones:
                candidates.extend(chunk_phones)
                used_chunks.append(i)
        
        if not candidates:
            logger.debug("No phone numbers found")
            return None, 0.0, []
        
        # If we have multiple candidates, try to disambiguate
        if len(candidates) > 1:
            best_phone = await self._disambiguate_phones(candidates, chunks)
        else:
            best_phone = candidates[0]
        
        # Normalize the phone number
        normalized_phone = self._normalize_phone(best_phone)
        confidence = self._calculate_confidence(best_phone, chunks)
        
        logger.debug("Phone extraction completed", phone=normalized_phone, confidence=confidence)
        
        return normalized_phone, confidence, used_chunks
    
    def _extract_phones_from_chunk(self, chunk: str) -> List[str]:
        """Extract phone numbers from a single chunk."""
        phones = []
        
        for pattern in self.patterns:
            matches = pattern.findall(chunk)
            phones.extend(matches)
        
        # Also try utility function
        phones.extend(parse_phone_variants(chunk))
        
        # Remove duplicates and clean up
        unique_phones = []
        for phone in phones:
            cleaned = re.sub(r'[-\s()]', '', phone)
            if cleaned not in [re.sub(r'[-\s()]', '', p) for p in unique_phones]:
                unique_phones.append(phone)
        
        return unique_phones
    
    async def _disambiguate_phones(self, candidates: List[str], chunks: List[str]) -> str:
        """Disambiguate between multiple phone number candidates."""
        
        # Simple heuristics first
        mobile_candidates = [p for p in candidates if self._is_mobile_number(p)]
        if len(mobile_candidates) == 1:
            return mobile_candidates[0]
        
        # If we have multiple mobile numbers, try LLM
        try:
            text = "\n".join(chunks[:2])  # Use first 2 chunks
            
            user_prompt = f"""From this restaurant text, identify the main contact phone number:

{text}

Available phone numbers: {', '.join(candidates)}

Return the most likely main contact number."""
            
            # Simple schema for phone disambiguation
            schema = {
                "type": "object",
                "properties": {
                    "phone": {"type": ["string", "null"]},
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "required": ["phone", "confidence"]
            }
            
            result = await generate_json(
                user_prompt=user_prompt,
                schema=schema,
                temperature=0.1
            )
            
            selected_phone = result.get("phone")
            if selected_phone and selected_phone in candidates:
                logger.debug("LLM selected phone", phone=selected_phone)
                return selected_phone
                
        except (LLMDisabled, Exception) as e:
            logger.debug("LLM disambiguation failed, using heuristics", error=str(e))
        
        # Fallback: prefer mobile numbers, then longest number
        if mobile_candidates:
            return mobile_candidates[0]
        
        return max(candidates, key=len)
    
    def _is_mobile_number(self, phone: str) -> bool:
        """Check if phone number is a mobile number."""
        # Remove formatting
        digits = re.sub(r'[^\d]', '', phone)
        
        # Indian mobile numbers start with 6-9 and are 10 digits
        if len(digits) == 10:
            return digits[0] in '6789'
        elif len(digits) == 12 and digits.startswith('91'):
            return digits[2] in '6789'
        elif len(digits) == 13 and digits.startswith('+91'):
            return digits[3] in '6789'
        
        return False
    
    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number to +91XXXXXXXXXX format."""
        # Remove all non-digit characters
        digits = re.sub(r'[^\d]', '', phone)
        
        # Handle different formats
        if len(digits) == 10 and digits[0] in '6789':
            # 10-digit mobile
            return f"+91{digits}"
        elif len(digits) == 12 and digits.startswith('91'):
            # 91XXXXXXXXXX format
            return f"+{digits}"
        elif len(digits) == 13 and digits.startswith('91'):
            # 91XXXXXXXXXXX (13 digits, probably error)
            return f"+91{digits[2:]}"
        elif len(digits) >= 10:
            # Try to extract 10 digits starting with 6-9
            for i in range(len(digits) - 9):
                candidate = digits[i:i+10]
                if candidate[0] in '6789':
                    return f"+91{candidate}"
        
        # Return as-is if can't normalize
        return phone
    
    def _calculate_confidence(self, phone: str, chunks: List[str]) -> float:
        """Calculate confidence score for extracted phone."""
        confidence = 0.0
        
        # Base confidence for having a phone
        confidence += 0.3
        
        # Boost if it's a properly formatted mobile number
        if self._is_mobile_number(phone):
            confidence += 0.2
        
        # Boost if it appears near phone indicators
        text = " ".join(chunks).lower()
        for indicator in self.phone_indicators:
            if re.search(indicator, text):
                context_match = re.search(f'{indicator}.*?{re.escape(phone)}', text)
                if context_match:
                    confidence += 0.3
                    break
                # Check reverse order too
                reverse_match = re.search(f'{re.escape(phone)}.*?{indicator}', text)
                if reverse_match:
                    confidence += 0.2
                    break
        
        # Boost if number appears multiple times (consistency)
        phone_clean = re.sub(r'[^\d]', '', phone)
        count = sum(1 for chunk in chunks if phone_clean in re.sub(r'[^\d]', '', chunk))
        if count > 1:
            confidence += 0.1
        
        return min(confidence, 1.0)


# Global extractor instance
phone_extractor = PhoneExtractor()


async def extract_phone(chunks: List[str]) -> Tuple[Optional[str], float, List[int]]:
    """Extract phone number from text chunks."""
    return await phone_extractor.extract(chunks)
