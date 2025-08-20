"""
Base scraper class for web scraping with configurable HTML selectors.
Uses JSON configuration files to define HTML element selectors.
"""

import json
import os
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup, Tag
from .base_session import BaseSession


class BaseScraper(ABC):
    """
    Abstract base class for web scrapers.
    Uses JSON configuration files to define HTML selectors.
    """
    
    def __init__(self, 
                 session: BaseSession,
                 config_name: str,
                 config_dir: str = "configs"):
        """
        Initialize the base scraper.
        
        Args:
            session: Session object for making HTTP requests
            config_name: Name of the JSON config file (without .json extension)
            config_dir: Directory containing config files
        """
        self.session = session
        self.config_name = config_name
        self.config_dir = config_dir
        self.selectors = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load HTML selectors from JSON config file."""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            self.config_dir,
            f"{self.config_name}.json"
        )
        
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
    
    def get_soup(self, url: str) -> BeautifulSoup:
        """
        Get BeautifulSoup object from URL.
        
        Args:
            url: URL to scrape
            
        Returns:
            BeautifulSoup object
        """
        response = self.session.get(url)
        return BeautifulSoup(response.text, 'html.parser')
    
    def extract_elements(self, 
                        soup: BeautifulSoup, 
                        selector_path: str) -> List[Tag]:
        """
        Extract elements using selector path from config.
        
        Args:
            soup: BeautifulSoup object
            selector_path: Dot-notation path to selector in config (e.g., "film_list.container")
            
        Returns:
            List of matched elements
        """
        selector = self._get_nested_selector(selector_path)
        if not selector:
            raise ValueError(f"Selector not found: {selector_path}")
        
        return soup.select(selector)
    
    def extract_text(self, 
                    element: Tag, 
                    selector_path: str,
                    attribute: Optional[str] = None) -> Optional[str]:
        """
        Extract text or attribute from element using selector.
        
        Args:
            element: BeautifulSoup element
            selector_path: Dot-notation path to selector in config
            attribute: HTML attribute name to extract (if None, extracts text)
            
        Returns:
            Extracted text or attribute value
        """
        selector = self._get_nested_selector(selector_path)
        if not selector:
            return None
        
        found_element = element.select_one(selector)
        if not found_element:
            return None
        
        if attribute:
            return found_element.get(attribute)
        return found_element.get_text(strip=True)
    
    def _get_nested_selector(self, path: str) -> Optional[str]:
        """
        Get selector from nested config using dot notation.
        
        Args:
            path: Dot-notation path (e.g., "film_list.container")
            
        Returns:
            CSS selector string
        """
        keys = path.split('.')
        current = self.selectors
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        
        return current if isinstance(current, str) else None
    
    @abstractmethod
    def scrape(self, url: str) -> Dict[str, Any]:
        """
        Main scraping method. Must be implemented by subclasses.
        
        Args:
            url: URL to scrape
            
        Returns:
            Scraped data
        """
        pass
