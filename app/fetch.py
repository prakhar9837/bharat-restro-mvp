"""HTTP client for fetching web content with caching and rate limiting."""

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import httpx
from requests_cache import CachedSession

from .config import settings
from .log import logger
from .robots import robots_checker
from .utils import hash_content, hash_url, safe_filename


class WebContentFetcher:
    """Fetch web content with caching, rate limiting, and robots.txt compliance."""
    
    def __init__(self):
        self.session = self._create_cached_session()
        self.last_request_times: Dict[str, float] = {}
        self.raw_data_dir = settings.raw_data_dir
        
        # Ensure directories exist
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
    
    def _create_cached_session(self) -> CachedSession:
        """Create cached HTTP session."""
        cache_file = settings.http_cache_dir / "http_cache"
        
        session = CachedSession(
            cache_name=str(cache_file),
            backend='sqlite',
            expire_after=86400,  # 24 hours
        )
        
        session.headers.update({
            'User-Agent': settings.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        return session
    
    async def fetch_url(self, url: str, timeout: Optional[int] = None) -> Tuple[Optional[bytes], Dict[str, any]]:
        """Fetch URL content with metadata."""
        timeout = timeout or settings.timeout_seconds
        
        logger.info("Fetching URL", url=url)
        
        # Check robots.txt
        if not robots_checker.can_fetch(url):
            logger.warning("Robots.txt disallows fetching", url=url)
            return None, {
                "url": url,
                "status": "robots_disallowed",
                "fetched_at": time.time(),
                "error": "Disallowed by robots.txt"
            }
        
        # Rate limiting
        await self._apply_rate_limit(url)
        
        try:
            # Use httpx for async support
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(
                    url,
                    headers={'User-Agent': settings.user_agent},
                    follow_redirects=True
                )
                
                content = response.content
                metadata = {
                    "url": url,
                    "final_url": str(response.url),
                    "status_code": response.status_code,
                    "content_type": response.headers.get("content-type", ""),
                    "content_length": len(content),
                    "content_hash": hash_content(content),
                    "fetched_at": time.time(),
                }
                
                if response.status_code >= 400:
                    logger.warning("HTTP error", url=url, status_code=response.status_code)
                    metadata["error"] = f"HTTP {response.status_code}"
                    return None, metadata
                
                # Save to disk
                self._save_raw_content(content, metadata)
                
                logger.info(
                    "Successfully fetched URL",
                    url=url,
                    status_code=response.status_code,
                    content_length=len(content)
                )
                
                return content, metadata
                
        except Exception as e:
            logger.error("Failed to fetch URL", url=url, error=str(e))
            return None, {
                "url": url,
                "status": "error",
                "fetched_at": time.time(),
                "error": str(e)
            }
    
    def _save_raw_content(self, content: bytes, metadata: Dict[str, any]) -> None:
        """Save raw content and metadata to disk."""
        try:
            content_hash = metadata["content_hash"]
            
            # Save content
            content_file = self.raw_data_dir / f"{content_hash}.bin"
            with open(content_file, 'wb') as f:
                f.write(content)
            
            # Save metadata
            metadata_file = self.raw_data_dir / f"{content_hash}.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            logger.debug("Saved raw content", content_hash=content_hash, size=len(content))
            
        except Exception as e:
            logger.error("Failed to save raw content", error=str(e))
    
    async def _apply_rate_limit(self, url: str) -> None:
        """Apply rate limiting per host."""
        from urllib.parse import urlparse
        
        parsed = urlparse(url)
        host = parsed.netloc
        
        current_time = time.time()
        last_request = self.last_request_times.get(host, 0)
        
        # Calculate required delay
        min_interval = 1.0 / settings.rate_limit_per_host
        time_since_last = current_time - last_request
        
        if time_since_last < min_interval:
            delay = min_interval - time_since_last
            logger.debug("Rate limiting", host=host, delay=delay)
            await asyncio.sleep(delay)
        
        self.last_request_times[host] = time.time()
    
    def get_cached_content(self, url: str) -> Tuple[Optional[bytes], Optional[Dict[str, any]]]:
        """Get cached content if available."""
        url_hash = hash_url(url)
        
        # Look for existing files with this URL hash in metadata
        for metadata_file in self.raw_data_dir.glob("*.json"):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                if metadata.get("url") == url:
                    # Found matching metadata, load content
                    content_hash = metadata.get("content_hash")
                    if content_hash:
                        content_file = self.raw_data_dir / f"{content_hash}.bin"
                        if content_file.exists():
                            with open(content_file, 'rb') as f:
                                content = f.read()
                            return content, metadata
                            
            except Exception as e:
                logger.debug("Error reading cached file", file=str(metadata_file), error=str(e))
                continue
        
        return None, None


async def fetch_urls(urls: list[str], concurrency: int = 4) -> Dict[str, Tuple[Optional[bytes], Dict[str, any]]]:
    """Fetch multiple URLs with controlled concurrency."""
    fetcher = WebContentFetcher()
    
    results = {}
    semaphore = asyncio.Semaphore(concurrency)
    
    async def fetch_single(url: str) -> None:
        async with semaphore:
            content, metadata = await fetcher.fetch_url(url)
            results[url] = (content, metadata)
    
    # Create tasks for all URLs
    tasks = [fetch_single(url) for url in urls]
    
    # Wait for all to complete
    await asyncio.gather(*tasks)
    
    logger.info("Batch fetch completed", total_urls=len(urls), successful=sum(1 for content, _ in results.values() if content))
    
    return results
