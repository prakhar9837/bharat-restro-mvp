"""Robots.txt checker for polite web scraping."""

import urllib.robotparser
from typing import Dict, Optional
from urllib.parse import urljoin, urlparse

from .config import settings
from .log import logger


class RobotsChecker:
    """Check robots.txt compliance before making requests."""
    
    def __init__(self):
        self._robots_cache: Dict[str, urllib.robotparser.RobotFileParser] = {}
    
    def can_fetch(self, url: str, user_agent: Optional[str] = None) -> bool:
        """Check if URL can be fetched according to robots.txt."""
        try:
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            # Get robots.txt parser for this domain
            rp = self._get_robots_parser(base_url)
            
            if not rp:
                # If we can't fetch robots.txt, allow the request
                logger.warning("Could not fetch robots.txt", url=base_url)
                return True
            
            user_agent = user_agent or settings.user_agent
            can_fetch = rp.can_fetch(user_agent, url)
            
            logger.debug(
                "Robots.txt check", 
                url=url, 
                user_agent=user_agent, 
                can_fetch=can_fetch
            )
            
            return can_fetch
            
        except Exception as e:
            logger.error("Error checking robots.txt", url=url, error=str(e))
            # On error, allow the request to avoid blocking legitimate requests
            return True
    
    def _get_robots_parser(self, base_url: str) -> Optional[urllib.robotparser.RobotFileParser]:
        """Get robots.txt parser for base URL with caching."""
        if base_url in self._robots_cache:
            return self._robots_cache[base_url]
        
        try:
            robots_url = urljoin(base_url, "/robots.txt")
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            
            self._robots_cache[base_url] = rp
            logger.debug("Loaded robots.txt", robots_url=robots_url)
            
            return rp
            
        except Exception as e:
            logger.warning("Failed to load robots.txt", base_url=base_url, error=str(e))
            # Cache None to avoid repeated failures
            self._robots_cache[base_url] = None
            return None
    
    def get_crawl_delay(self, url: str, user_agent: Optional[str] = None) -> Optional[float]:
        """Get crawl delay from robots.txt."""
        try:
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            rp = self._get_robots_parser(base_url)
            if not rp:
                return None
                
            user_agent = user_agent or settings.user_agent
            delay = rp.crawl_delay(user_agent)
            
            return float(delay) if delay else None
            
        except Exception as e:
            logger.error("Error getting crawl delay", url=url, error=str(e))
            return None


# Global robots checker instance
robots_checker = RobotsChecker()
