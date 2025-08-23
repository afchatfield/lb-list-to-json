"""
Optimized Letterboxd scraper with reduced code duplication.
Uses utility classes for common operations and parallel processing.
"""

from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Any, Callable
import sys
import os
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_session import BaseSession
from core.base_scraper import BaseScraper
from core.letterboxd_utils import (
    FilmDataExtractor, StatsExtractor, ListFilmExtractor, 
    PaginationHelper
)
from core.parallel_processor import ParallelProcessor, BatchProcessor


class LetterboxdSession(BaseSession):
    """Session class specifically for Letterboxd.com"""
    
    def __init__(self):
        super().__init__(
            base_url="https://letterboxd.com",
            headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            },
            delay=0.2
        )
        self.configure_for_site()
    
    def configure_for_site(self) -> None:
        """Configure session specifically for Letterboxd.com"""
        pass


class LetterboxdScraper(BaseScraper):
    """
    Optimized scraper for Letterboxd content.
    Uses utility classes to reduce code duplication and improve maintainability.
    """
    
    # Predefined lists for easy access
    PREDEFINED_LISTS = {
        "my_top_100": ("el_duderinno", "my-top-100"),
        "all_the_films": ("hershwin", "all-the-movies"),
        "letterboxd_250": ("dave", "official-top-250-narrative-feature-films"),
        "letterboxd_250_docs": ("dave", "official-top-250-documentary-films"),
    }
    
    def __init__(self):
        self.letterboxd_session = LetterboxdSession()
        
        # Initialize utility classes
        self.film_extractor = FilmDataExtractor()
        self.stats_extractor = StatsExtractor()
        self.list_extractor = ListFilmExtractor()
        self.pagination_helper = PaginationHelper()
        
        # Initialize parallel processors
        self.parallel_processor = ParallelProcessor(
            LetterboxdSession, 
            self.letterboxd_session.base_url,
            self.letterboxd_session.session.headers,
            self.letterboxd_session.delay
        )
        self.batch_processor = BatchProcessor(LetterboxdSession, self.parallel_processor)
        
        # Initialize base scraper
        try:
            super().__init__(
                session=self.letterboxd_session,
                config_name="letterboxd_selectors"
            )
        except FileNotFoundError:
            self.selectors = {}
            logging.warning("Letterboxd selectors config not found, using empty selectors")
    
    def scrape(self, url: str) -> dict:
        """Basic scrape method implementation (required by base class)."""
        soup = self.get_soup(url)
        return {"soup": soup, "url": url}
    
    def get_page_soup(self, suffix: str) -> BeautifulSoup:
        """Get a BeautifulSoup object for any Letterboxd page."""
        if not suffix.startswith('/'):
            suffix = '/' + suffix
        
        response = self.letterboxd_session.get(suffix)
        if response is None:
            raise Exception(f"Failed to get response for {suffix}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        logging.info(f"Successfully retrieved and parsed page: {suffix}")
        return soup
    
    def get_list_soup(self, username: str, list_name: str) -> BeautifulSoup:
        """Get BeautifulSoup object for a specific user's list."""
        suffix = f"/{username}/list/{list_name}/"
        return self.get_page_soup(suffix)
    
    def get_film_soup(self, film_slug: str) -> BeautifulSoup:
        """Get BeautifulSoup object for a specific film page."""
        suffix = f"/film/{film_slug}/"
        return self.get_page_soup(suffix)
    
    def test_connection(self) -> bool:
        """Test connection to Letterboxd.com"""
        try:
            soup = self.get_page_soup("/")
            if soup.find("title") and "letterboxd" in soup.find("title").text.lower():
                logging.info("Successfully connected to Letterboxd.com")
                return True
            else:
                logging.error("Connected but didn't get expected Letterboxd content")
                return False
        except Exception as e:
            logging.error(f"Failed to connect to Letterboxd.com: {e}")
            return False
    
    # ==================== LIST SCRAPING METHODS ====================
    
    def get_films_from_list(self, username: str, list_name: str) -> List[Dict[str, Any]]:
        """Get films from a single page of a user's list."""
        soup = self.get_list_soup(username, list_name)
        return self.list_extractor.extract_films_from_list(soup)
    
    def get_list_pagination_info(self, username: str, list_name: str) -> Dict[str, int]:
        """Get pagination information for a list."""
        soup = self.get_list_soup(username, list_name)
        return self.pagination_helper.get_pagination_info(soup)
    
    def get_all_films_from_list_sequential(self, username: str, list_name: str,
                                         page_progress_callback: Optional[Callable] = None,
                                         film_progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Get all films from a list sequentially (non-parallel)."""
        return self.batch_processor.collect_all_basic_films(username, list_name, page_progress_callback)
    
    def get_all_films_from_list_parallel(self, username: str, list_name: str,
                                       max_workers: Optional[int] = None,
                                       page_progress_callback: Optional[Callable] = None,
                                       film_progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Get all films from a list using parallel processing."""
        # Get pagination info first
        pagination_info = self.get_list_pagination_info(username, list_name)
        total_pages = pagination_info['total_pages']
        
        # Prepare tasks for parallel processing
        page_tasks = self.batch_processor.prepare_page_tasks(username, list_name, total_pages)
        
        # Execute in parallel
        return self.parallel_processor.scrape_pages_parallel(
            page_tasks, max_workers, page_progress_callback
        )
    
    # ==================== FILM DETAIL METHODS ====================
    
    def get_film_details(self, film_slug: str) -> Dict[str, Any]:
        """Get detailed information for a specific film."""
        soup = self.get_film_soup(film_slug)
        return self.film_extractor.extract_basic_film_data(soup)
    
    def get_all_films_with_details_sequential(self, username: str, list_name: str,
                                            page_progress_callback: Optional[Callable] = None,
                                            film_progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Get all films with details sequentially."""
        # First get basic film data
        basic_films = self.get_all_films_from_list_sequential(username, list_name, page_progress_callback)
        
        # Then get details for each film
        detailed_films = []
        total_films = len(basic_films)
        
        for i, film in enumerate(basic_films, 1):
            if film_progress_callback:
                film_progress_callback(i, total_films, 
                                     f"Getting details for film {i}/{total_films}: {film.get('name', 'Unknown')}")
            
            film_slug = film.get('film_slug')
            if film_slug:
                try:
                    detailed_film = self.get_film_details(film_slug)
                    detailed_film.update(film)
                    detailed_films.append(detailed_film)
                except Exception as e:
                    logging.error(f"Error getting details for {film_slug}: {e}")
                    detailed_films.append(film)
            else:
                detailed_films.append(film)
        
        return detailed_films
    
    def get_all_films_with_details_parallel(self, username: str, list_name: str,
                                          max_workers: Optional[int] = None,
                                          page_progress_callback: Optional[Callable] = None,
                                          film_progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Get all films with details using parallel processing."""
        # First get basic film data
        basic_films = self.get_all_films_from_list_parallel(username, list_name, max_workers, page_progress_callback)
        
        # Then get details in parallel
        return self.parallel_processor.get_film_details_parallel(basic_films, max_workers, film_progress_callback)
    
    # ==================== OPTIMIZED TWO-PHASE METHODS ====================
    
    def get_all_films_optimized(self, username: str, list_name: str,
                               page_progress_callback: Optional[Callable] = None,
                               film_progress_callback: Optional[Callable] = None,
                               max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Optimized two-phase scraping: collect basic info, then get details.
        This is the recommended method for most use cases.
        """
        # Phase 1: Collect all basic film info quickly
        basic_films = self.batch_processor.collect_all_basic_films(
            username, list_name, page_progress_callback
        )
        
        if not basic_films:
            return []
        
        # Phase 2: Get detailed info in parallel
        return self.parallel_processor.get_film_details_parallel(
            basic_films, max_workers, film_progress_callback
        )
    
    # ==================== RATINGS AND STATS METHODS ====================
    
    def get_film_ratings_soup(self, film_slug: str) -> BeautifulSoup:
        """Get BeautifulSoup object for a film's ratings summary page."""
        suffix = f"/csi/film/{film_slug}/ratings-summary/"
        return self.get_page_soup(suffix)
    
    def get_film_stats_soup(self, film_slug: str) -> BeautifulSoup:
        """Get BeautifulSoup object for a film's stats page."""
        suffix = f"/csi/film/{film_slug}/stats/"
        return self.get_page_soup(suffix)
    
    def get_film_ratings_and_stats(self, film_slug: str) -> Dict[str, Any]:
        """Get comprehensive ratings and statistics data for a film."""
        combined_data = {'film_slug': film_slug}
        
        try:
            # Get ratings data
            ratings_soup = self.get_film_ratings_soup(film_slug)
            ratings_data = self.film_extractor.extract_ratings_data(ratings_soup)
            combined_data.update(ratings_data)
            
            # Get stats data
            stats_soup = self.get_film_stats_soup(film_slug)
            stats_data = self.stats_extractor.extract_stats_data(stats_soup)
            combined_data.update(stats_data)
            
        except Exception as e:
            logging.error(f"Error getting ratings and stats for {film_slug}: {e}")
        
        return combined_data
    
    def get_all_films_ratings_stats_only(self, username: str, list_name: str,
                                        page_progress_callback: Optional[Callable] = None,
                                        film_progress_callback: Optional[Callable] = None,
                                        max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get only ratings and stats data for all films in a list.
        This is much faster than getting full film details.
        """
        # Phase 1: Collect basic film info
        basic_films = self.batch_processor.collect_all_basic_films(
            username, list_name, page_progress_callback
        )
        
        if not basic_films:
            return []
        
        # Phase 2: Get ratings and stats in parallel
        return self.parallel_processor.get_ratings_stats_parallel(
            basic_films, max_workers, film_progress_callback
        )
    
    # ==================== LEGACY COMPATIBILITY METHODS ====================
    
    def extract_film_data_from_list(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Legacy method for backward compatibility."""
        return self.list_extractor.extract_films_from_list(soup)
    
    def extract_film_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Legacy method for backward compatibility."""
        return self.film_extractor.extract_basic_film_data(soup)
    
    def extract_film_ratings_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Legacy method for backward compatibility."""
        return self.film_extractor.extract_ratings_data(soup)
    
    def extract_film_stats_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Legacy method for backward compatibility."""
        return self.stats_extractor.extract_stats_data(soup)
