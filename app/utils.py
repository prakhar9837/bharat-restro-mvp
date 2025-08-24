"""Utility functions for the application."""

import hashlib
import time
from pathlib import Path
from typing import Any, Union


def hash_content(content: Union[str, bytes]) -> str:
    """Generate SHA256 hash of content."""
    if isinstance(content, str):
        content = content.encode('utf-8')
    return hashlib.sha256(content).hexdigest()


def hash_url(url: str) -> str:
    """Generate hash for URL to use as filename."""
    return hash_content(url)[:16]


def safe_filename(name: str, max_length: int = 255) -> str:
    """Convert string to safe filename."""
    # Remove/replace unsafe characters
    safe_chars = []
    for char in name:
        if char.isalnum() or char in '.-_':
            safe_chars.append(char)
        else:
            safe_chars.append('_')
    
    filename = ''.join(safe_chars)
    
    # Truncate if too long
    if len(filename) > max_length:
        filename = filename[:max_length-8] + '_' + hash_content(name)[:7]
    
    return filename


def ensure_dir(path: Path) -> Path:
    """Ensure directory exists and return the path."""
    path.mkdir(parents=True, exist_ok=True)
    return path


class Timer:
    """Simple timer context manager."""
    
    def __init__(self, name: str = "operation"):
        self.name = name
        self.start_time = 0.0
        self.end_time = 0.0
    
    def __enter__(self) -> 'Timer':
        self.start_time = time.time()
        return self
    
    def __exit__(self, *args: Any) -> None:
        self.end_time = time.time()
    
    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time


def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text to max length with ellipsis."""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."


def parse_phone_variants(text: str) -> list[str]:
    """Extract potential phone number variants from text."""
    import re
    
    # Pattern for Indian phone numbers
    patterns = [
        r'\+91[-\s]?[6-9]\d{9}',  # +91 format
        r'91[-\s]?[6-9]\d{9}',    # 91 format  
        r'[6-9]\d{9}',            # 10-digit mobile
        r'0\d{2,4}[-\s]?\d{6,8}', # Landline with STD code
    ]
    
    phones = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        phones.extend(matches)
    
    return list(set(phones))  # Remove duplicates
