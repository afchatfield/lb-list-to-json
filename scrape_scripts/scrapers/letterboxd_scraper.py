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
from core.config_loader import selector_config
from core.letterboxd_utils import (
    FilmDataExtractor, StatsExtractor, ListFilmExtractor, 
    PaginationHelper, BrowseFilmExtractor
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
        
        # Load selectors from centralized config
        selectors = selector_config.get_selectors()
        
        # Initialize base scraper with loaded selectors
        try:
            super().__init__(
                session=self.letterboxd_session,
                config_name="letterboxd_selectors"
            )
        except FileNotFoundError:
            # Use the centralized config if file loading fails
            self.selectors = selectors
            logging.warning("Using centralized selector config as fallback")
        
        # Initialize utility classes with selectors from config
        film_selectors = selectors.get('film_page', {})
        stats_selectors = selectors.get('stats_csi', {})
        
        self.film_extractor = FilmDataExtractor(film_selectors)
        self.stats_extractor = StatsExtractor(stats_selectors)
        self.list_extractor = ListFilmExtractor(selectors)
        self.pagination_helper = PaginationHelper(selectors)
        self.browse_extractor = BrowseFilmExtractor(
            selectors.get('browse_films', BrowseFilmExtractor.DEFAULT_SELECTORS)
        )
        
        # Initialize parallel processors
        self.parallel_processor = ParallelProcessor(
            LetterboxdSession, 
            self.letterboxd_session.base_url,
            self.letterboxd_session.session.headers,
            self.letterboxd_session.delay,
            film_selectors=film_selectors,
            stats_selectors=stats_selectors,
        )
        self.batch_processor = BatchProcessor(LetterboxdSession, self.parallel_processor)
    
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
        """Get comprehensive ratings and statistics data for a film.
        
        Tries /csi/ endpoints first. Falls back to main page meta tag extraction
        if /csi/ endpoints return 403/fail.
        """
        combined_data = {'film_slug': film_slug}
        csi_success = False
        
        try:
            # Try ratings from /csi/ endpoint
            try:
                ratings_soup = self.get_film_ratings_soup(film_slug)
                ratings_data = self.film_extractor.extract_ratings_data(ratings_soup)
                if ratings_data:
                    combined_data.update(ratings_data)
                    csi_success = True
            except Exception as e:
                logging.debug(f"CSI ratings endpoint failed for {film_slug}: {e}")
            
            # Try stats from /csi/ endpoint
            try:
                stats_soup = self.get_film_stats_soup(film_slug)
                stats_data = self.stats_extractor.extract_stats_data(stats_soup)
                if stats_data:
                    combined_data.update(stats_data)
                    csi_success = True
            except Exception as e:
                logging.debug(f"CSI stats endpoint failed for {film_slug}: {e}")
            
            # Fallback: extract average_rating from main page meta tag
            if not csi_success:
                logging.warning(f"Both /csi/ endpoints failed for {film_slug}, trying meta tag fallback")
                try:
                    soup = self.get_film_soup(film_slug)
                    meta_data = self.film_extractor.extract_ratings_from_meta(soup)
                    if meta_data:
                        combined_data.update(meta_data)
                    else:
                        logging.error(
                            f"ALL extraction methods failed for {film_slug}: "
                            f"/csi/ endpoints and meta tag fallback all returned no data"
                        )
                except Exception as e:
                    logging.error(f"Meta tag fallback also failed for {film_slug}: {e}")
        
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
    
    # ==================== BROWSE/COUNTRY FILMS METHODS ====================
    
    def scrape_browse_films(self, production_country: str = None, rating_country: str = None,
                           language: str = None,
                           sort: str = 'rating', limit: int = None,
                           delay: float = 2.5,
                           progress_callback=None,
                           known_slugs: set = None) -> List[Dict[str, Any]]:
        """
        Scrape films from the Letterboxd browse/AJAX pages with country/language filters.
        
        Args:
            production_country: Country slug for films produced in (e.g. 'france', 'italy'). Optional.
            rating_country: Country slug for weighting by that country's ratings. Optional.
            language: Language slug (e.g. 'french', 'italian'). Optional.
            sort: Sort method ('rating', 'popular', etc.)
            limit: Max number of *matched* films to collect. None = all available.
                   When known_slugs is provided, limit counts only films that match
                   the filter. Scraping continues until enough matches are found.
            delay: Delay between page requests (2.5s recommended to avoid Cloudflare)
            progress_callback: Optional callback(page, total_pages, matched, total)
            known_slugs: Optional set of film slugs to filter against. When provided,
                        the limit applies to matched films only.
        
        Returns:
            List of film dicts with keys: film_slug, name, name_with_year, film_id,
            year, average_rating, target_link, browse_rank
        """
        import time
        all_films = []
        page = 1
        total_pages = None  # None = unknown (browse pages don't have numbered pagination)
        original_delay = self.letterboxd_session.delay
        
        # Fresh session to avoid stale Cloudflare tokens from prior requests
        self.letterboxd_session.refresh_session()
        
        # Use slower delay for browse pages to avoid Cloudflare
        self.letterboxd_session.delay = delay
        
        try:
            while True:
                url = BrowseFilmExtractor.build_ajax_url(production_country, rating_country, language, sort, page)
                full_url = f"{self.letterboxd_session.base_url.rstrip('/')}/{url.lstrip('/')}"
                logging.info(f"Fetching browse page {page}: {full_url}")
                
                try:
                    # Use the cloudscraper session directly (not BaseSession.get) to avoid
                    # retry logic interfering with Cloudflare challenge-solving
                    response = self.letterboxd_session.session.get(
                        full_url, timeout=self.letterboxd_session.timeout
                    )
                    
                    if response.status_code == 403:
                        # Refresh the session to get a new TLS fingerprint and retry
                        logging.warning(f"Got 403 on page {page}, refreshing session and retrying after {delay * 2}s...")
                        time.sleep(delay * 2)
                        self.letterboxd_session.refresh_session()
                        response = self.letterboxd_session.session.get(
                            full_url, timeout=self.letterboxd_session.timeout
                        )
                    
                    if response.status_code == 403:
                        # Second attempt: longer wait + another refresh
                        logging.warning(f"Still 403 on page {page}, second refresh after {delay * 3}s...")
                        time.sleep(delay * 3)
                        self.letterboxd_session.refresh_session()
                        response = self.letterboxd_session.session.get(
                            full_url, timeout=self.letterboxd_session.timeout
                        )
                    
                    if response.status_code != 200:
                        logging.warning(f"Page {page} returned status {response.status_code}")
                        if page == 1:
                            raise Exception(f"Failed to fetch page 1: HTTP {response.status_code}")
                        break
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                except Exception as e:
                    logging.warning(f"Failed to fetch page {page}: {e}")
                    if page == 1:
                        raise  # First page failure is fatal
                    break
                
                # Check for Cloudflare challenge
                title_el = soup.select_one('title')
                if title_el and 'moment' in title_el.get_text().lower():
                    logging.warning(f"Cloudflare challenge on page {page}, stopping")
                    break
                
                # Get total pages from first page (may be None if no numbered pagination)
                if page == 1:
                    total_pages = self.browse_extractor.get_total_pages(soup)
                    if total_pages:
                        logging.info(f"Total pages detected: {total_pages}")
                    else:
                        logging.info("No numbered pagination found — will paginate using next/prev links")
                
                # Extract films — start_rank continues from previous pages
                page_films = self.browse_extractor.extract_films_from_browse(
                    soup, start_rank=len(all_films) + 1
                )
                logging.info(f"Page {page}: extracted {len(page_films)} films")
                
                if not page_films:
                    logging.info(f"No films on page {page}, stopping")
                    break
                
                all_films.extend(page_films)
                
                # Count matched films if filtering
                if known_slugs is not None:
                    matched_count = sum(1 for f in all_films if f.get('film_slug') in known_slugs)
                else:
                    matched_count = len(all_films)
                
                if progress_callback:
                    progress_callback(page, total_pages, matched_count, len(all_films))
                
                # Check if we have enough (matched films, not raw total)
                if limit and matched_count >= limit:
                    logging.info(f"Reached {matched_count} matched films (limit: {limit}), stopping")
                    break
                
                # Check for next page
                has_next = self.browse_extractor.has_next_page(soup)
                if not has_next:
                    logging.info(f"No next page after page {page}, stopping")
                    break
                
                # Rate limit between pages with random jitter to appear less bot-like
                import random
                jitter = random.uniform(0.5, 1.5)
                actual_delay = delay + jitter
                logging.info(f"Waiting {actual_delay:.1f}s before next page...")
                time.sleep(actual_delay)
                
                page += 1
        finally:
            self.letterboxd_session.delay = original_delay
        
        return all_films
    
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
