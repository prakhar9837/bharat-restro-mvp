"""Content parsing for HTML, PDF, and OCR with chunk extraction."""

import json
import re
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from lxml import html
from pdfminer.high_level import extract_text_to_fp
from pdfminer.layout import LAParams
from PIL import Image
import pytesseract

from .config import settings
from .log import logger
from .utils import hash_content


class ContentChunk:
    """Represents a chunk of extracted text with metadata."""
    
    def __init__(
        self, 
        text: str, 
        start_offset: int = 0, 
        end_offset: int = 0,
        chunk_type: str = "text",
        source_info: Optional[Dict] = None
    ):
        self.text = text.strip()
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.chunk_type = chunk_type
        self.source_info = source_info or {}
    
    def to_dict(self) -> Dict[str, any]:
        """Convert chunk to dictionary."""
        return {
            "text": self.text,
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
            "chunk_type": self.chunk_type,
            "source_info": self.source_info,
            "length": len(self.text)
        }


class HTMLParser:
    """Parse HTML content and extract relevant sections."""
    
    def __init__(self):
        # Keywords that indicate relevant sections
        self.relevant_keywords = [
            "contact", "address", "location", "find us", "reach us",
            "hours", "timing", "open", "closed", "schedule",
            "phone", "call", "mobile", "telephone",
            "about", "menu", "cuisine", "food", "dining"
        ]
    
    def parse(self, content: bytes, url: str = "") -> List[ContentChunk]:
        """Parse HTML content and extract text chunks."""
        try:
            # Parse with BeautifulSoup for robustness
            soup = BeautifulSoup(content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            chunks = []
            
            # Extract title
            title = soup.find('title')
            if title and title.string:
                chunks.append(ContentChunk(
                    text=title.string,
                    chunk_type="title",
                    source_info={"url": url, "element": "title"}
                ))
            
            # Extract relevant sections by headings
            chunks.extend(self._extract_by_headings(soup, url))
            
            # Extract by keywords in text
            chunks.extend(self._extract_by_keywords(soup, url))
            
            # Extract structured data
            chunks.extend(self._extract_structured_data(soup, url))
            
            # Fallback: extract all text if we found very little
            if len(chunks) < 3:
                full_text = soup.get_text()
                if full_text:
                    chunks.append(ContentChunk(
                        text=full_text,
                        chunk_type="fallback",
                        source_info={"url": url, "element": "body"}
                    ))
            
            logger.debug("HTML parsing completed", url=url, chunks_found=len(chunks))
            return chunks
            
        except Exception as e:
            logger.error("HTML parsing failed", url=url, error=str(e))
            return []
    
    def _extract_by_headings(self, soup: BeautifulSoup, url: str) -> List[ContentChunk]:
        """Extract content sections by headings."""
        chunks = []
        
        for heading_tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            headings = soup.find_all(heading_tag)
            
            for heading in headings:
                heading_text = heading.get_text().strip()
                
                if any(keyword in heading_text.lower() for keyword in self.relevant_keywords):
                    # Extract content after this heading
                    content_parts = [heading_text]
                    
                    # Get next siblings until another heading
                    for sibling in heading.next_siblings:
                        if hasattr(sibling, 'name'):
                            if sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                                break
                            text = sibling.get_text().strip()
                            if text:
                                content_parts.append(text)
                    
                    if len(content_parts) > 1:
                        chunks.append(ContentChunk(
                            text="\n".join(content_parts),
                            chunk_type="section",
                            source_info={
                                "url": url, 
                                "element": heading_tag,
                                "heading": heading_text
                            }
                        ))
        
        return chunks
    
    def _extract_by_keywords(self, soup: BeautifulSoup, url: str) -> List[ContentChunk]:
        """Extract content by keyword matching."""
        chunks = []
        
        # Find divs/sections with relevant keywords in class or id
        for element in soup.find_all(['div', 'section', 'article', 'aside']):
            class_str = ' '.join(element.get('class', []))
            id_str = element.get('id', '')
            combined = f"{class_str} {id_str}".lower()
            
            if any(keyword in combined for keyword in self.relevant_keywords):
                text = element.get_text().strip()
                if text and len(text) > 20:  # Minimum content length
                    chunks.append(ContentChunk(
                        text=text,
                        chunk_type="keyword_section",
                        source_info={
                            "url": url,
                            "element": element.name,
                            "class": class_str,
                            "id": id_str
                        }
                    ))
        
        return chunks
    
    def _extract_structured_data(self, soup: BeautifulSoup, url: str) -> List[ContentChunk]:
        """Extract structured data like JSON-LD, microdata."""
        chunks = []
        
        # Extract JSON-LD structured data
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get('@type') in ['Restaurant', 'FoodEstablishment']:
                    chunks.append(ContentChunk(
                        text=json.dumps(data, indent=2),
                        chunk_type="structured_data",
                        source_info={"url": url, "format": "json-ld"}
                    ))
            except Exception:
                continue
        
        return chunks


class PDFParser:
    """Parse PDF content and extract text."""
    
    def parse(self, content: bytes, source_url: str = "") -> List[ContentChunk]:
        """Parse PDF content and extract text chunks."""
        try:
            # Use pdfminer to extract text
            output_string = BytesIO()
            with BytesIO(content) as input_file:
                extract_text_to_fp(
                    input_file, 
                    output_string,
                    laparams=LAParams(),
                    output_type='text',
                    codec=None
                )
            
            text = output_string.getvalue().decode('utf-8')
            
            if not text.strip():
                logger.warning("No text extracted from PDF", source_url=source_url)
                return []
            
            # Split into pages or sections
            chunks = []
            
            # Try to split by page breaks or form feeds
            pages = re.split(r'\f|\n\s*\n\s*\n', text)
            
            for i, page_text in enumerate(pages):
                page_text = page_text.strip()
                if page_text and len(page_text) > 50:  # Minimum content
                    chunks.append(ContentChunk(
                        text=page_text,
                        chunk_type="pdf_page",
                        source_info={
                            "source_url": source_url,
                            "page_number": i + 1
                        }
                    ))
            
            logger.debug("PDF parsing completed", source_url=source_url, chunks_found=len(chunks))
            return chunks
            
        except Exception as e:
            logger.error("PDF parsing failed", source_url=source_url, error=str(e))
            return []


class OCRParser:
    """Parse images using OCR."""
    
    def parse(self, content: bytes, source_url: str = "") -> List[ContentChunk]:
        """Parse image content using OCR."""
        try:
            # Open image with PIL
            image = Image.open(BytesIO(content))
            
            # Extract text using Tesseract
            text = pytesseract.image_to_string(image, lang='eng')
            
            if not text.strip():
                logger.warning("No text extracted via OCR", source_url=source_url)
                return []
            
            # Create single chunk for OCR text
            chunk = ContentChunk(
                text=text,
                chunk_type="ocr_text",
                source_info={
                    "source_url": source_url,
                    "image_size": image.size
                }
            )
            
            logger.debug("OCR parsing completed", source_url=source_url, text_length=len(text))
            return [chunk]
            
        except Exception as e:
            logger.error("OCR parsing failed", source_url=source_url, error=str(e))
            return []


class ContentParser:
    """Main content parser that routes to appropriate parsers."""
    
    def __init__(self):
        self.html_parser = HTMLParser()
        self.pdf_parser = PDFParser()
        self.ocr_parser = OCRParser()
        self.parsed_data_dir = settings.parsed_data_dir
        
        # Ensure directory exists
        self.parsed_data_dir.mkdir(parents=True, exist_ok=True)
    
    def parse_content(
        self, 
        content: bytes, 
        content_type: str, 
        source_url: str = ""
    ) -> List[ContentChunk]:
        """Parse content based on content type."""
        
        if not content:
            return []
        
        # Determine parser based on content type
        content_type = content_type.lower()
        
        if 'html' in content_type or 'xml' in content_type:
            chunks = self.html_parser.parse(content, source_url)
        elif 'pdf' in content_type:
            chunks = self.pdf_parser.parse(content, source_url)
        elif 'image' in content_type:
            chunks = self.ocr_parser.parse(content, source_url)
        else:
            # Try to detect format from content
            if content.startswith(b'%PDF'):
                chunks = self.pdf_parser.parse(content, source_url)
            elif b'<html' in content[:1000].lower() or b'<!doctype' in content[:1000].lower():
                chunks = self.html_parser.parse(content, source_url)
            else:
                logger.warning("Unknown content type", content_type=content_type, source_url=source_url)
                return []
        
        # Save parsed chunks
        if chunks:
            self._save_parsed_chunks(chunks, source_url)
        
        return chunks
    
    def _save_parsed_chunks(self, chunks: List[ContentChunk], source_url: str) -> None:
        """Save parsed chunks to disk."""
        try:
            content_hash = hash_content(source_url)
            parsed_file = self.parsed_data_dir / f"{content_hash}.json"
            
            chunks_data = {
                "source_url": source_url,
                "chunks": [chunk.to_dict() for chunk in chunks],
                "parsed_at": json.dumps({"timestamp": "now"}),  # Simplified
                "total_chunks": len(chunks)
            }
            
            with open(parsed_file, 'w', encoding='utf-8') as f:
                json.dump(chunks_data, f, indent=2, ensure_ascii=False)
            
            logger.debug("Saved parsed chunks", source_url=source_url, chunks_count=len(chunks))
            
        except Exception as e:
            logger.error("Failed to save parsed chunks", source_url=source_url, error=str(e))


def parse_file_content(file_path: Path) -> List[ContentChunk]:
    """Parse content from a file."""
    parser = ContentParser()
    
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        
        # Determine content type from extension
        suffix = file_path.suffix.lower()
        content_type_map = {
            '.html': 'text/html',
            '.htm': 'text/html',
            '.pdf': 'application/pdf',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
        }
        
        content_type = content_type_map.get(suffix, 'application/octet-stream')
        
        return parser.parse_content(content, content_type, str(file_path))
        
    except Exception as e:
        logger.error("Failed to parse file", file_path=str(file_path), error=str(e))
        return []
