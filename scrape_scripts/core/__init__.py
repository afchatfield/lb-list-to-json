"""
Core module for Letterboxd scraping framework.
Contains base classes for session management, scraping, and parsing.
"""

from .base_session import BaseSession
from .base_scraper import BaseScraper
from .base_parser import BaseParser

__all__ = ['BaseSession', 'BaseScraper', 'BaseParser']
