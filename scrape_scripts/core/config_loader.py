"""
Configuration loader for dynamic selector management.
Provides centralized access to selector configurations.
"""

import json
import os
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class SelectorConfig:
    """Centralized configuration management for selectors."""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SelectorConfig, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._config = self._load_selectors()
    
    def _load_selectors(self) -> Dict[str, Any]:
        """Load selectors from the JSON configuration file."""
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'configs',
            'letterboxd_selectors.json'
        )
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                logger.info(f"Successfully loaded selectors from {config_path}")
                return config
        except FileNotFoundError:
            logger.error(f"Selectors config file not found: {config_path}")
            return self._get_default_selectors()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in selectors config: {e}")
            return self._get_default_selectors()
    
    def _get_default_selectors(self) -> Dict[str, Any]:
        """Return default selectors as fallback."""
        return {
            "film_list": {
                "container": ".js-list-entries",
                "film_item": "li.poster-container",
                "film_poster_div": "div.poster",
                "film_name": "img",
                "film_link": "[data-target-link]",
                "film_id": "[data-film-id]",
                "film_slug": "[data-film-slug]",
                "list_number": "p.list-number",
                "owner_rating": "[data-owner-rating]",
                "poster_container": ".poster-container",
                "data_item_slug": "div[data-item-slug]",
                "data_film_slug": "div[data-film-slug]",
                "data_film_id": "div[data-film-id]",
                "film_img": "img"
            },
            "film_page": {
                "title": "h1.headline-1 .name",
                "year": ".releasedate a",
                "director": ".credits .prettify",
                "runtime": "p.text-link",
                "synopsis": ".production-synopsis .prettify p",
                "tagline": ".tagline",
                "original_title": ".originalname",
                "cast": ".cast-list .text-slug",
                "genres": "#tab-genres .text-slug",
                "countries": "#tab-details .text-sluglist a[href*='/films/country/']",
                "primary_language": "#tab-details .text-sluglist a[href*='/films/language/']:first-of-type",
                "other_languages": "#tab-details .text-sluglist a[href*='/films/language/']",
                "studios": "#tab-details .text-sluglist a[href*='/studio/']",
                "rating_section": "section.ratings-histogram-chart",
                "average_rating": ".average-rating .display-rating",
                "fans_link": "a[href*=\"/fans/\"]",
                "histogram_bars": ".rating-histogram-bar a"
            },
            "pagination": {
                "pagination_container": ".pagination",
                "page_links": ".pagination li a",
                "last_page": ".pagination li:last-child a"
            },
            "attributes": {
                "data_item_slug": "data-item-slug",
                "data_film_slug": "data-film-slug",
                "data_film_id": "data-film-id",
                "data_item_name": "data-item-name",
                "data_film_name": "data-film-name",
                "data_owner_rating": "data-owner-rating",
                "data_target_link": "data-target-link",
                "data_item_link": "data-item-link",
                "alt": "alt"
            }
        }
    
    def get_selectors(self) -> Dict[str, Any]:
        """Get the complete selectors configuration."""
        return self._config.copy()
    
    def get_film_list_selectors(self) -> Dict[str, str]:
        """Get selectors for film list pages."""
        return self._config.get('film_list', {})
    
    def get_film_page_selectors(self) -> Dict[str, str]:
        """Get selectors for individual film pages."""
        return self._config.get('film_page', {})
    
    def get_pagination_selectors(self) -> Dict[str, str]:
        """Get selectors for pagination elements."""
        return self._config.get('pagination', {})
    
    def get_attributes(self) -> Dict[str, str]:
        """Get attribute names for data extraction."""
        return self._config.get('attributes', {})
    
    def get_selector(self, path: str) -> Optional[str]:
        """
        Get a specific selector using dot notation.
        
        Args:
            path: Dot-notation path (e.g., "film_list.container")
            
        Returns:
            CSS selector string or None if not found
        """
        keys = path.split('.')
        current = self._config
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        
        return current if isinstance(current, str) else None
    
    def reload_config(self) -> None:
        """Reload the configuration from file."""
        self._config = self._load_selectors()
        logger.info("Selectors configuration reloaded")


# Global instance for easy access
selector_config = SelectorConfig()


def get_selectors() -> Dict[str, Any]:
    """Convenience function to get selectors."""
    return selector_config.get_selectors()


def get_selector(path: str) -> Optional[str]:
    """Convenience function to get a specific selector."""
    return selector_config.get_selector(path)
