"""
Base session class for efficient web scraping with requests.
Handles session management, headers, rate limiting, and error handling.
"""

import requests
import time
import logging
from typing import Dict, Optional, Any
from abc import ABC, abstractmethod


class BaseSession(ABC):
    """
    Abstract base class for web scraping sessions.
    Provides common functionality for HTTP requests with session management.
    """
    
    def __init__(self, 
                 base_url: str = "",
                 headers: Optional[Dict[str, str]] = None,
                 delay: float = 1.0,
                 timeout: int = 30,
                 max_retries: int = 3):
        """
        Initialize the base session.
        
        Args:
            base_url: Base URL for the target website
            headers: Default headers for requests
            delay: Delay between requests (seconds)
            timeout: Request timeout (seconds)
            max_retries: Maximum number of retry attempts
        """
        self.base_url = base_url
        self.delay = delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        
        # Set default headers
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        if headers:
            default_headers.update(headers)
        
        self.session.headers.update(default_headers)
        
        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """
        Perform GET request with retry logic and rate limiting.
        
        Args:
            url: URL to request (can be relative to base_url)
            **kwargs: Additional arguments passed to requests.get
            
        Returns:
            Response object
            
        Raises:
            requests.RequestException: If all retry attempts fail
        """
        full_url = self._build_url(url)
        
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.debug(f"Requesting: {full_url} (attempt {attempt + 1})")
                
                response = self.session.get(
                    full_url, 
                    timeout=self.timeout,
                    **kwargs
                )
                response.raise_for_status()
                
                # Rate limiting
                if self.delay > 0:
                    time.sleep(self.delay)
                
                return response
                
            except requests.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries + 1}): {e}")
                
                if attempt == self.max_retries:
                    self.logger.error(f"All retry attempts failed for: {full_url}")
                    raise
                
                # Exponential backoff
                time.sleep(2 ** attempt)
    
    def _build_url(self, url: str) -> str:
        """Build full URL from base_url and relative URL."""
        if url.startswith('http'):
            return url
        return f"{self.base_url.rstrip('/')}/{url.lstrip('/')}"
    
    def close(self):
        """Close the session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    @abstractmethod
    def configure_for_site(self) -> None:
        """Configure session for specific website. Must be implemented by subclasses."""
        pass
