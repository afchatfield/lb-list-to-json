"""
Letterboxd scraper for extracting movie data from Letterboxd.com
Handles lists, films, and other Letterboxd content.
"""

from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Any, Callable
import sys
import os
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import time
from functools import partial
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_session import BaseSession
from core.base_scraper import BaseScraper


class LetterboxdSession(BaseSession):
    """Session class specifically for Letterboxd.com"""
    
    def __init__(self):
        super().__init__(
            base_url="https://letterboxd.com",
            headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            },
            delay=0.2  # Be respectful to Letterboxd servers
        )
        self.configure_for_site()
    
    def configure_for_site(self) -> None:
        """Configure session specifically for Letterboxd.com"""
        # Additional Letterboxd-specific configuration can go here
        pass


class LetterboxdScraper(BaseScraper):
    """
    Scraper for Letterboxd content including lists and films.
    """
    
    # Predefined lists for easy access
    PREDEFINED_LISTS = {
        "my_top_100": ("el_duderinno", "my-top-100"),
        "all_the_films": ("hershwin", "all-the-movies"),
        "letterboxd_250": ("dave", "official-top-250-narrative-feature-films"),
        "letterboxd_250_docs": ("dave", "official-top-250-documentary-films"),
    }
    
    def __init__(self):
        # Create a Letterboxd session
        self.letterboxd_session = LetterboxdSession()
        
        # Initialize with letterboxd config (we'll create this config file)
        try:
            super().__init__(
                session=self.letterboxd_session,
                config_name="letterboxd_selectors"
            )
        except FileNotFoundError:
            # If config doesn't exist yet, initialize with empty selectors
            self.selectors = {}
            logging.warning("Letterboxd selectors config not found, using empty selectors")
    
    def scrape(self, url: str) -> dict:
        """
        Basic scrape method implementation (required by base class).
        For now, just returns the soup object.
        """
        soup = self.get_soup(url)
        return {"soup": soup, "url": url}
    
    def get_page_soup(self, suffix: str) -> BeautifulSoup:
        """
        Get a BeautifulSoup object for any Letterboxd page.
        
        Args:
            suffix: The URL suffix after letterboxd.com (e.g., '/film/the-phoenician-scheme/')
        
        Returns:
            BeautifulSoup object of the page HTML
        """
        # Ensure suffix starts with /
        if not suffix.startswith('/'):
            suffix = '/' + suffix
        
        # Make the request
        response = self.letterboxd_session.get(suffix)
        
        if response is None:
            raise Exception(f"Failed to get response for {suffix}")
        
        # Create and return BeautifulSoup object
        soup = BeautifulSoup(response.text, 'html.parser')
        logging.info(f"Successfully retrieved and parsed page: {suffix}")
        
        return soup
    
    def get_list_soup(self, username: str, list_name: str) -> BeautifulSoup:
        """
        Get BeautifulSoup object for a specific user's list.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
        
        Returns:
            BeautifulSoup object of the list page
        """
        suffix = f"/{username}/list/{list_name}/"
        return self.get_page_soup(suffix)
    
    def get_film_soup(self, film_slug: str) -> BeautifulSoup:
        """
        Get BeautifulSoup object for a specific film page.
        
        Args:
            film_slug: Film slug (e.g., 'the-phoenician-scheme')
        
        Returns:
            BeautifulSoup object of the film page
        """
        suffix = f"/film/{film_slug}/"
        return self.get_page_soup(suffix)
    
    def test_connection(self) -> bool:
        """
        Test connection to Letterboxd.com
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            soup = self.get_page_soup("/")
            # Check if we got a valid Letterboxd page
            if soup.find("title") and "letterboxd" in soup.find("title").text.lower():
                logging.info("Successfully connected to Letterboxd.com")
                return True
            else:
                logging.error("Connected but didn't get expected Letterboxd content")
                return False
        except Exception as e:
            logging.error(f"Failed to connect to Letterboxd.com: {e}")
            return False
    
    def extract_film_data_from_list(self, soup: BeautifulSoup) -> list:
        """
        Extract film data from a list page using JSON selectors.
        
        Args:
            soup: BeautifulSoup object of the list page
            
        Returns:
            List of dictionaries containing film data
        """
        films = []
        
        # Get all film items from the list
        film_items = soup.select(self.selectors["film_list"]["film_item"])
        
        for item in film_items:
            film_data = self._extract_film_from_item(item)
            if film_data:
                films.append(film_data)
        
        return films
    
    def _extract_film_from_item(self, item) -> dict:
        """
        Extract individual film data from a list item using JSON selectors.
        
        Args:
            item: BeautifulSoup element representing a film item
            
        Returns:
            Dictionary containing film data
        """
        try:
            # Get the poster div which contains most data attributes
            poster_div = item.select_one(self.selectors["film_list"]["film_poster_div"])
            if not poster_div:
                return None
            
            # Extract basic film information
            film_data = {
                "film_id": poster_div.get("data-film-id"),
                "film_slug": poster_div.get("data-film-slug"),
                "target_link": poster_div.get("data-target-link"),
                "name": None,
                "list_position": None,
                "owner_rating": item.get("data-owner-rating")
            }
            
            # Extract film name from img alt attribute
            img_element = poster_div.select_one(self.selectors["film_list"]["film_name"])
            if img_element:
                film_data["name"] = img_element.get("alt")
            
            # Extract list position
            list_number_element = item.select_one(self.selectors["film_list"]["list_number"])
            if list_number_element:
                film_data["list_position"] = list_number_element.get_text(strip=True)
            
            return film_data
            
        except Exception as e:
            logging.error(f"Error extracting film data from item: {e}")
            return None
    
    def extract_film_details(self, soup: BeautifulSoup) -> dict:
        """
        Extract detailed film information from a film page using JSON selectors.
        
        Args:
            soup: BeautifulSoup object of the film page
            
        Returns:
            Dictionary containing detailed film data
        """
        film_details = {}
        
        try:
            # Extract title
            title_element = soup.select_one(self.selectors["film_page"]["title"])
            if title_element:
                film_details["title"] = title_element.get_text(strip=True)
            
            # Extract year
            year_element = soup.select_one(self.selectors["film_page"]["year"])
            if year_element:
                film_details["year"] = year_element.get_text(strip=True)
            
            # Extract director
            director_element = soup.select_one(self.selectors["film_page"]["director"])
            if director_element:
                film_details["director"] = director_element.get_text(strip=True)
            
            # Extract original title
            original_title_element = soup.select_one(self.selectors["film_page"]["original_title"])
            if original_title_element:
                film_details["original_title"] = original_title_element.get_text(strip=True)
            
            # Extract tagline
            tagline_element = soup.select_one(self.selectors["film_page"]["tagline"])
            if tagline_element:
                film_details["tagline"] = tagline_element.get_text(strip=True)
            
            # Extract synopsis
            synopsis_element = soup.select_one(self.selectors["film_page"]["synopsis"])
            if synopsis_element:
                film_details["synopsis"] = synopsis_element.get_text(strip=True)
            
            # Extract lists of data
            film_details["cast"] = self._extract_text_list(soup, self.selectors["film_page"]["cast"])
            film_details["genres"] = self._extract_text_list(soup, self.selectors["film_page"]["genres"])
            film_details["countries"] = self._extract_text_list(soup, self.selectors["film_page"]["countries"])
            film_details["languages"] = self._extract_text_list(soup, self.selectors["film_page"]["languages"])
            film_details["studios"] = self._extract_text_list(soup, self.selectors["film_page"]["studios"])
            
        except Exception as e:
            logging.error(f"Error extracting film details: {e}")
        
        return film_details
    
    def _extract_text_list(self, soup: BeautifulSoup, selector: str) -> list:
        """
        Extract a list of text values from elements matching a selector.
        
        Args:
            soup: BeautifulSoup object
            selector: CSS selector string
            
        Returns:
            List of text values
        """
        try:
            elements = soup.select(selector)
            return [element.get_text(strip=True) for element in elements if element.get_text(strip=True)]
        except Exception as e:
            logging.error(f"Error extracting text list with selector '{selector}': {e}")
            return []
    
    def get_films_from_list(self, username: str, list_name: str) -> list:
        """
        Get all films from a user's list with extracted data.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
            
        Returns:
            List of dictionaries containing film data
        """
        soup = self.get_list_soup(username, list_name)
        return self.extract_film_data_from_list(soup)
    
    def get_film_details(self, film_slug: str) -> dict:
        """
        Get detailed information for a specific film.
        
        Args:
            film_slug: Film slug (e.g., 'parasite-2019')
            
        Returns:
            Dictionary containing detailed film data
        """
        soup = self.get_film_soup(film_slug)
        return self.extract_film_details(soup)
    
    def get_predefined_list_films(self, list_key: str) -> list:
        """
        Get all films from a predefined list.
        
        Args:
            list_key: Key from PREDEFINED_LISTS
            
        Returns:
            List of dictionaries containing film data
        """
        if list_key not in self.PREDEFINED_LISTS:
            raise ValueError(f"Unknown predefined list: {list_key}. Available: {list(self.PREDEFINED_LISTS.keys())}")
        
        username, list_name = self.PREDEFINED_LISTS[list_key]
        return self.get_films_from_list(username, list_name)

    def get_list_pagination_info(self, username: str, list_name: str) -> dict:
        """
        Get pagination information for a list.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
            
        Returns:
            Dictionary with total_pages, total_films, films_per_page
        """
        soup = self.get_list_soup(username, list_name)
        
        # Check for pagination
        pagination = soup.find('div', class_='paginate-pages')
        total_pages = 1
        
        if pagination:
            page_links = pagination.find_all('a')
            if page_links:
                # Get the last page number
                last_page_link = page_links[-1]
                if 'page' in last_page_link.get('href', ''):
                    try:
                        total_pages = int(last_page_link.get('href').split('page/')[1].rstrip('/'))
                    except:
                        total_pages = 1
        
        # Count films on current page to estimate total
        films_on_page = len(soup.select('.poster-container'))
        total_films_estimate = films_on_page * total_pages
        
        return {
            'total_pages': total_pages,
            'total_films_estimate': total_films_estimate,
            'films_per_page': films_on_page
        }

    def get_all_films_from_list_paginated(self, username: str, list_name: str, 
                                        page_progress_callback=None, 
                                        film_progress_callback=None) -> list:
        """
        Get all films from a list, handling pagination.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
            page_progress_callback: Optional callback for page progress updates
            film_progress_callback: Optional callback for film progress updates
            
        Returns:
            List of dictionaries containing basic film data from all pages
        """
        # Get pagination info
        pagination_info = self.get_list_pagination_info(username, list_name)
        total_pages = pagination_info['total_pages']
        
        all_films = []
        
        for page in range(1, total_pages + 1):
            if page_progress_callback:
                page_progress_callback(page, total_pages, f"Scraping page {page}/{total_pages}")
            
            # Get page URL
            if page == 1:
                page_url = f"/{username}/list/{list_name}/"
            else:
                page_url = f"/{username}/list/{list_name}/page/{page}/"
            
            try:
                soup = self.get_page_soup(page_url)
                films_on_page = self.extract_film_data_from_list(soup)
                
                # Update film progress for each film on this page
                if film_progress_callback:
                    for i, film in enumerate(films_on_page):
                        film_progress_callback(i + 1, len(films_on_page), 
                                             f"Processing: {film.get('name', 'Unknown')}")
                
                all_films.extend(films_on_page)
                
                logging.info(f"Extracted {len(films_on_page)} films from page {page}")
                
            except Exception as e:
                logging.error(f"Error scraping page {page}: {e}")
                continue
        
        return all_films

    def get_all_films_with_details_paginated(self, username: str, list_name: str,
                                           page_progress_callback=None,
                                           film_progress_callback=None) -> list:
        """
        Get all films from a list with full details, handling pagination.
        
        Args:
            username: Letterboxd username  
            list_name: Name of the list
            page_progress_callback: Optional callback for page progress updates
            film_progress_callback: Optional callback for film progress updates
            
        Returns:
            List of dictionaries containing detailed film data
        """
        # First get all basic film data
        basic_films = self.get_all_films_from_list_paginated(username, list_name, 
                                                           page_progress_callback, 
                                                           film_progress_callback)
        
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
                    # Merge basic list data with detailed film data
                    detailed_film.update(film)
                    detailed_films.append(detailed_film)
                    
                except Exception as e:
                    logging.error(f"Error getting details for {film_slug}: {e}")
                    # Fall back to basic film data
                    detailed_films.append(film)
            else:
                detailed_films.append(film)
        
        return detailed_films

    def _scrape_single_page(self, page_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Scrape a single page of films. Used for parallel processing.
        
        Args:
            page_info: Dictionary containing page, username, list_name, and url
            
        Returns:
            List of film dictionaries from this page
        """
        page = page_info['page']
        username = page_info['username']
        list_name = page_info['list_name']
        page_url = page_info['url']
        
        try:
            # Create a new session for this worker (thread-safe)
            session = LetterboxdSession()
            response = session.get(page_url)
            
            if response is None:
                logging.error(f"Failed to get response for page {page}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            films_on_page = self.extract_film_data_from_list(soup)
            
            logging.info(f"Extracted {len(films_on_page)} films from page {page}")
            return films_on_page
            
        except Exception as e:
            logging.error(f"Error scraping page {page}: {e}")
            return []

    def get_all_films_from_list_parallel(self, username: str, list_name: str,
                                       max_workers: Optional[int] = None,
                                       page_progress_callback=None,
                                       film_progress_callback=None) -> List[Dict[str, Any]]:
        """
        Get all films from a list using parallel processing for pages.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
            max_workers: Maximum number of worker threads (default: min(8, cpu_count))
            page_progress_callback: Optional callback for page progress updates
            film_progress_callback: Optional callback for film progress updates
            
        Returns:
            List of dictionaries containing basic film data from all pages
        """
        # Get pagination info
        pagination_info = self.get_list_pagination_info(username, list_name)
        total_pages = pagination_info['total_pages']
        
        if max_workers is None:
            max_workers = min(8, multiprocessing.cpu_count())
        
        logging.info(f"Using {max_workers} workers for {total_pages} pages")
        
        # Prepare page URLs for parallel processing
        page_tasks = []
        for page in range(1, total_pages + 1):
            if page == 1:
                page_url = f"/{username}/list/{list_name}/"
            else:
                page_url = f"/{username}/list/{list_name}/page/{page}/"
            
            page_tasks.append({
                'page': page,
                'username': username,
                'list_name': list_name,
                'url': page_url
            })
        
        all_films = []
        completed_pages = 0
        
        # Use ThreadPoolExecutor for I/O bound tasks (web scraping)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all page scraping tasks
            future_to_page = {
                executor.submit(self._scrape_single_page, page_info): page_info['page']
                for page_info in page_tasks
            }
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                completed_pages += 1
                
                try:
                    films_from_page = future.result()
                    all_films.extend(films_from_page)
                    
                    # Update progress
                    if page_progress_callback:
                        page_progress_callback(completed_pages, total_pages, 
                                             f"Completed page {completed_pages}/{total_pages}")
                    
                    # Update film progress
                    if film_progress_callback:
                        for i, film in enumerate(films_from_page):
                            film_progress_callback(i + 1, len(films_from_page),
                                                 f"Processing: {film.get('name', 'Unknown')}")
                    
                except Exception as e:
                    logging.error(f"Page {page_num} generated an exception: {e}")
        
        # Sort films by their original order (list_position if available)
        try:
            all_films.sort(key=lambda x: int(x.get('list_position', 0)))
        except (ValueError, TypeError):
            # If sorting fails, keep original order
            pass
        
        logging.info(f"Parallel processing completed: {len(all_films)} films from {total_pages} pages")
        return all_films

    def get_all_films_with_details_parallel(self, username: str, list_name: str,
                                          max_workers: Optional[int] = None,
                                          page_progress_callback=None,
                                          film_progress_callback=None) -> List[Dict[str, Any]]:
        """
        Get all films from a list with detailed info using parallel processing.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
            max_workers: Maximum number of worker threads
            page_progress_callback: Optional callback for page progress updates
            film_progress_callback: Optional callback for film progress updates
            
        Returns:
            List of dictionaries containing detailed film data
        """
        # First get basic film data using parallel processing
        basic_films = self.get_all_films_from_list_parallel(username, list_name, 
                                                          max_workers, 
                                                          page_progress_callback,
                                                          film_progress_callback)
        
        # Now get detailed info for each film using parallel processing
        if max_workers is None:
            max_workers = min(6, multiprocessing.cpu_count())  # Slightly fewer for detail requests
        
        logging.info(f"Getting detailed info for {len(basic_films)} films using {max_workers} workers")
        
        detailed_films = []
        completed_films = 0
        
        def get_film_details_safe(film: Dict[str, Any]) -> Dict[str, Any]:
            """Thread-safe function to get film details."""
            film_slug = film.get('film_slug')
            if not film_slug:
                return film
            
            try:
                # Create new session for thread safety
                session = LetterboxdSession()
                temp_scraper = LetterboxdScraper()
                temp_scraper.letterboxd_session = session
                
                detailed_film = temp_scraper.get_film_details(film_slug)
                detailed_film.update(film)  # Merge with basic data
                return detailed_film
                
            except Exception as e:
                logging.error(f"Error getting details for {film_slug}: {e}")
                return film  # Return basic data on error
        
        # Use ThreadPoolExecutor for detailed film scraping
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all film detail tasks
            future_to_film = {
                executor.submit(get_film_details_safe, film): film
                for film in basic_films
            }
            
            # Process completed tasks
            for future in as_completed(future_to_film):
                original_film = future_to_film[future]
                completed_films += 1
                
                try:
                    detailed_film = future.result()
                    detailed_films.append(detailed_film)
                    
                    # Update progress
                    if film_progress_callback:
                        film_progress_callback(completed_films, len(basic_films),
                                             f"Details: {original_film.get('name', 'Unknown')}")
                    
                except Exception as e:
                    logging.error(f"Film detail task generated an exception: {e}")
                    detailed_films.append(original_film)  # Use basic data
        
        # Sort films by their original order
        try:
            detailed_films.sort(key=lambda x: int(x.get('list_position', 0)))
        except (ValueError, TypeError):
            pass
        
        logging.info(f"Parallel detail processing completed: {len(detailed_films)} films")
        return detailed_films

    # ==================== OPTIMIZED TWO-PHASE SCRAPING METHODS ====================
    
    def get_all_films_optimized(self, username: str, list_name: str,
                               page_progress_callback: Optional[Callable] = None,
                               film_progress_callback: Optional[Callable] = None,
                               max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Optimized two-phase scraping: first collect all basic film info quickly,
        then get detailed info for all films.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
            page_progress_callback: Callback for page progress
            film_progress_callback: Callback for film detail progress
            max_workers: Number of workers for parallel processing
            
        Returns:
            List of dictionaries containing detailed film data
        """
        # Phase 1: Quickly collect all basic film info and slugs
        basic_films = self._collect_all_basic_films_fast(
            username, list_name, page_progress_callback
        )
        
        if not basic_films:
            return []
        
        # Phase 2: Get detailed info for all films in parallel
        detailed_films = self._get_detailed_info_parallel(
            basic_films, film_progress_callback, max_workers
        )
        
        return detailed_films
    
    def _collect_all_basic_films_fast(self, username: str, list_name: str,
                                    page_progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Phase 1: Quickly collect basic film info from all pages.
        Optimized for speed with minimal processing per page.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
            page_progress_callback: Callback for progress updates
            
        Returns:
            List of basic film data with slugs for detailed processing
        """
        # Get pagination info
        pagination_info = self.get_list_pagination_info(username, list_name)
        total_pages = pagination_info['total_pages']
        
        all_films = []
        global_position = 1  # Track position across all pages
        
        for page in range(1, total_pages + 1):
            try:
                # Build URL for this page
                page_url = f"{self.session.base_url}/{username}/list/{list_name}/page/{page}/"
                
                # Get page content
                response = self.session.get(page_url)
                if response is None:
                    continue
                
                # Quick parsing - minimal processing
                soup = BeautifulSoup(response.text, 'html.parser')
                films_on_page = self._extract_basic_films_fast(soup, global_position)
                
                # Add page-specific info and update global position
                for film in films_on_page:
                    film['source_page'] = page
                    film['source_list'] = f"{username}/{list_name}"
                    global_position += 1
                
                all_films.extend(films_on_page)
                
                # Progress callback
                if page_progress_callback:
                    page_progress_callback(page, total_pages, f"Collected {len(films_on_page)} films")
                
            except Exception as e:
                logging.error(f"Error collecting films from page {page}: {e}")
                continue
        
        logging.info(f"Phase 1 complete: Collected {len(all_films)} films from {total_pages} pages")
        return all_films
    
    def _extract_basic_films_fast(self, soup: BeautifulSoup, start_position: int = 1) -> List[Dict[str, Any]]:
        """
        Fast extraction of basic film info focusing only on essential data.
        Optimized for speed over completeness.
        
        Args:
            soup: BeautifulSoup object of the list page
            start_position: Starting position number for this page
            
        Returns:
            List of basic film dictionaries
        """
        films = []
        current_position = start_position
        
        # Use optimized selector - get all film posters at once
        film_posters = soup.select('div[data-film-slug]')
        
        for poster in film_posters:
            try:
                # Extract only essential data for Phase 1
                film_slug = poster.get('data-film-slug')
                if not film_slug:
                    continue
                
                # Get film name from img alt or data attributes
                img = poster.select_one('img')
                film_name = None
                if img:
                    film_name = img.get('alt', '').strip()
                
                # Try to get from data attributes if img alt is empty
                if not film_name:
                    film_name = poster.get('data-film-name', 'Unknown')
                
                # Use calculated list position based on order on page
                list_position = current_position
                
                # Get owner rating from parent list item
                list_item = poster.find_parent('li')
                owner_rating = None
                if list_item:
                    owner_rating = list_item.get('data-owner-rating')
                
                film_data = {
                    'film_slug': film_slug,
                    'name': film_name,
                    'film_id': poster.get('data-film-id'),
                    'target_link': poster.get('data-target-link'),
                    'list_position': list_position,
                    'owner_rating': owner_rating
                }
                
                films.append(film_data)
                current_position += 1
                
            except Exception as e:
                logging.debug(f"Error extracting basic film data: {e}")
                continue
        
        return films
    
    def _get_detailed_info_parallel(self, basic_films: List[Dict[str, Any]],
                                  film_progress_callback: Optional[Callable] = None,
                                  max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Phase 2: Get detailed info for all films in parallel.
        Optimized for maximum throughput.
        
        Args:
            basic_films: List of basic film data with slugs
            film_progress_callback: Callback for progress updates
            max_workers: Number of workers for parallel processing
            
        Returns:
            List of detailed film dictionaries
        """
        if not basic_films:
            return []
        
        # Filter films that have slugs
        films_with_slugs = [f for f in basic_films if f.get('film_slug')]
        
        if not films_with_slugs:
            return basic_films
        
        # Determine optimal worker count
        if max_workers is None:
            max_workers = min(8, multiprocessing.cpu_count(), len(films_with_slugs))
        
        detailed_films = []
        completed = 0
        
        logging.info(f"Phase 2: Getting detailed info for {len(films_with_slugs)} films using {max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_film = {
                executor.submit(self._get_film_details_safe, film): film
                for film in films_with_slugs
            }
            
            # Process completed tasks
            for future in as_completed(future_to_film):
                original_film = future_to_film[future]
                completed += 1
                
                try:
                    detailed_film = future.result()
                    if detailed_film:
                        # Merge basic data with detailed data
                        detailed_film.update(original_film)
                        detailed_films.append(detailed_film)
                    else:
                        # Fall back to basic data
                        detailed_films.append(original_film)
                        
                except Exception as e:
                    logging.error(f"Film detail task failed for {original_film.get('film_slug')}: {e}")
                    detailed_films.append(original_film)
                
                # Progress callback
                if film_progress_callback:
                    film_progress_callback(completed, len(films_with_slugs),
                                         f"Details: {original_film.get('name', 'Unknown')}")
        
        # Add films without slugs
        films_without_slugs = [f for f in basic_films if not f.get('film_slug')]
        detailed_films.extend(films_without_slugs)
        
        # Sort by list position if available
        try:
            detailed_films.sort(key=lambda x: int(x.get('list_position', 999999)))
        except (ValueError, TypeError):
            pass
        
        logging.info(f"Phase 2 complete: Processed {len(detailed_films)} films")
        return detailed_films
    
    def _get_film_details_safe(self, film: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Thread-safe method to get film details with error handling.
        
        Args:
            film: Basic film data containing film_slug
            
        Returns:
            Detailed film data or None if failed
        """
        film_slug = film.get('film_slug')
        if not film_slug:
            return None
        
        try:
            # Create thread-local session for thread safety
            session = LetterboxdSession()
            film_url = f"{session.base_url}/film/{film_slug}/"
            
            response = session.get(film_url)
            if response is None:
                return None
            
            # Fast parsing with optimized selectors
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract detailed film data efficiently
            detailed_data = self._extract_film_details_fast(soup)
            return detailed_data
            
        except Exception as e:
            logging.debug(f"Error getting details for {film_slug}: {e}")
            return None
    
    def _extract_film_details_fast(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Fast extraction of detailed film information with optimized selectors.
        
        Args:
            soup: BeautifulSoup object of the film page
            
        Returns:
            Dictionary containing detailed film data
        """
        film_data = {}
        
        try:
            # Use more specific selectors for faster parsing
            
            # Basic film info - use cached selectors
            title_elem = soup.select_one('h1.headline-1')
            if title_elem:
                film_data['title'] = title_elem.get_text(strip=True)
            
            # Year - look for specific pattern
            year_elem = soup.select_one('div.releaseyear a')
            if year_elem:
                try:
                    film_data['year'] = int(year_elem.get_text(strip=True))
                except ValueError:
                    pass
            
            # Director - optimized selector
            director_elem = soup.select_one('span.prettify a[href*="/director/"]')
            if director_elem:
                film_data['director'] = director_elem.get_text(strip=True)
            
            # Genres - batch extract
            genre_elems = soup.select('div.text-sluglist a[href*="/films/genre/"]')
            if genre_elems:
                film_data['genres'] = [elem.get_text(strip=True) for elem in genre_elems]
            
            # Runtime - look for specific pattern  
            runtime_elem = soup.select_one('p.text-link')
            if runtime_elem:
                runtime_text = runtime_elem.get_text(strip=True)
                if 'min' in runtime_text:
                    try:
                        film_data['runtime'] = int(runtime_text.split()[0])
                    except (ValueError, IndexError):
                        pass
            
            # Countries - batch extract
            country_elems = soup.select('div.text-sluglist a[href*="/films/country/"]')
            if country_elems:
                film_data['countries'] = [elem.get_text(strip=True) for elem in country_elems]
            
            # Cast - limit to first few for speed
            cast_elems = soup.select('div.text-sluglist a[href*="/actor/"]')[:10]  # Limit for speed
            if cast_elems:
                film_data['cast'] = [elem.get_text(strip=True) for elem in cast_elems]
            
            # Rating - look for average rating
            rating_elem = soup.select_one('section.ratings-histogram-chart')
            if rating_elem:
                rating_text = rating_elem.get('title', '')
                if 'average' in rating_text.lower():
                    try:
                        # Extract rating from title like "Average rating 4.1 (based on 12,345 ratings)"
                        import re
                        match = re.search(r'(\d+\.?\d*)', rating_text)
                        if match:
                            film_data['average_rating'] = float(match.group(1))
                    except (ValueError, AttributeError):
                        pass
            
        except Exception as e:
            logging.debug(f"Error in fast film detail extraction: {e}")
        
        return film_data

    # ==================== END OPTIMIZED METHODS ====================

    # ==================== RATINGS AND STATS SCRAPING METHODS ====================
    
    def get_film_ratings_soup(self, film_slug: str) -> BeautifulSoup:
        """
        Get BeautifulSoup object for a film's ratings summary page.
        
        Args:
            film_slug: Film slug (e.g., 'fallen-angels')
        
        Returns:
            BeautifulSoup object of the ratings summary page
        """
        suffix = f"/csi/film/{film_slug}/ratings-summary/"
        return self.get_page_soup(suffix)
    
    def get_film_stats_soup(self, film_slug: str) -> BeautifulSoup:
        """
        Get BeautifulSoup object for a film's stats page.
        
        Args:
            film_slug: Film slug (e.g., 'fallen-angels')
        
        Returns:
            BeautifulSoup object of the stats page
        """
        suffix = f"/csi/film/{film_slug}/stats/"
        return self.get_page_soup(suffix)
    
    def extract_film_ratings_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract detailed ratings information from a film's ratings summary page.
        
        Args:
            soup: BeautifulSoup object of the ratings summary page
            
        Returns:
            Dictionary containing detailed ratings data
        """
        ratings_data = {}
        
        try:
            # Extract ratings section - note that it has both 'section' and 'ratings-histogram-chart' classes
            ratings_section = soup.select_one('section.section.ratings-histogram-chart')
            if not ratings_section:
                return ratings_data
            
            # Extract average rating
            avg_rating_elem = ratings_section.select_one('.average-rating .display-rating')
            if avg_rating_elem:
                # Extract detailed rating info from title attribute first (more precise)
                title_attr = avg_rating_elem.get('title', '')
                if title_attr:
                    import re
                    # Extract precise average rating from title like "Weighted average of 4.17 based on 549,757 ratings"
                    avg_rating_match = re.search(r'Weighted average of ([\d.]+)', title_attr)
                    if avg_rating_match:
                        try:
                            ratings_data['average_rating'] = float(avg_rating_match.group(1))
                        except ValueError:
                            pass
                    
                    # Extract total ratings count from title like "Weighted average of 4.17 based on 549,757 ratings"
                    ratings_match = re.search(r'based on ([\d,]+)', title_attr)
                    if ratings_match:
                        ratings_data['total_ratings'] = int(ratings_match.group(1).replace(',', ''))
                
                # Fallback to displayed text if title parsing fails
                if 'average_rating' not in ratings_data:
                    try:
                        ratings_data['average_rating'] = float(avg_rating_elem.get_text(strip=True))
                    except ValueError:
                        pass
            
            # Extract fan count
            fans_link = ratings_section.select_one('a[href*="/fans/"]')
            if fans_link:
                fans_text = fans_link.get_text(strip=True)
                # Extract number from text like "37K fans"
                import re
                fans_match = re.search(r'([\d,]+(?:\.\d+)?)\s*([KM]?)', fans_text)
                if fans_match:
                    fans_num = float(fans_match.group(1).replace(',', ''))
                    multiplier = fans_match.group(2)
                    if multiplier == 'K':
                        fans_num *= 1000
                    elif multiplier == 'M':
                        fans_num *= 1000000
                    ratings_data['fans_count'] = int(fans_num)
            
            # Extract individual star ratings breakdown
            ratings_breakdown = {}
            histogram_bars = ratings_section.select('.rating-histogram-bar a')
            
            for bar in histogram_bars:
                title_attr = bar.get('title', '')
                if title_attr:
                    # Extract from titles like "152,638 ★★★★★ ratings (28%)"
                    import re
                    match = re.search(r'([\d,]+)\s*(★+(?:½)?)\s*ratings\s*\((\d+)%\)', title_attr)
                    if match:
                        count = int(match.group(1).replace(',', ''))
                        stars = match.group(2)
                        percentage = int(match.group(3))
                        
                        # Convert stars to numeric value
                        star_value = len(stars.replace('½', ''))
                        if '½' in stars:
                            star_value -= 0.5
                        
                        ratings_breakdown[f'stars_{star_value}'] = {
                            'count': count,
                            'percentage': percentage
                        }
            
            if ratings_breakdown:
                ratings_data['ratings_breakdown'] = ratings_breakdown
            
        except Exception as e:
            logging.error(f"Error extracting ratings data: {e}")
        
        return ratings_data
    
    def extract_film_stats_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract statistical information from a film's stats page.
        
        Args:
            soup: BeautifulSoup object of the stats page
            
        Returns:
            Dictionary containing statistical data
        """
        stats_data = {}
        
        try:
            # Find the production statistics list
            stats_container = soup.select_one('.production-statistic-list')
            if not stats_container:
                return stats_data
            
            # Extract watch count
            watches_stat = stats_container.select_one('.production-statistic.-watches')
            if watches_stat:
                watch_label = watches_stat.select_one('.label')
                if watch_label:
                    watch_text = watch_label.get_text(strip=True)
                    stats_data['watches_count'] = self._parse_stat_number(watch_text)
                
                # Also try to get from title attribute for exact count
                watch_link = watches_stat.select_one('a')
                if watch_link:
                    title_attr = watch_link.get('title', '')
                    import re
                    match = re.search(r'Watched by ([\d,]+)', title_attr)
                    if match:
                        stats_data['watches_count_exact'] = int(match.group(1).replace(',', ''))
            
            # Extract list appearances
            lists_stat = stats_container.select_one('.production-statistic.-lists')
            if lists_stat:
                lists_label = lists_stat.select_one('.label')
                if lists_label:
                    lists_text = lists_label.get_text(strip=True)
                    stats_data['lists_count'] = self._parse_stat_number(lists_text)
                
                # Also try to get from title attribute for exact count
                lists_link = lists_stat.select_one('a')
                if lists_link:
                    title_attr = lists_link.get('title', '')
                    import re
                    match = re.search(r'Appears in ([\d,]+)', title_attr)
                    if match:
                        stats_data['lists_count_exact'] = int(match.group(1).replace(',', ''))
            
            # Extract likes count
            likes_stat = stats_container.select_one('.production-statistic.-likes')
            if likes_stat:
                likes_label = likes_stat.select_one('.label')
                if likes_label:
                    likes_text = likes_label.get_text(strip=True)
                    stats_data['likes_count'] = self._parse_stat_number(likes_text)
                
                # Also try to get from title attribute for exact count
                likes_link = likes_stat.select_one('a')
                if likes_link:
                    title_attr = likes_link.get('title', '')
                    import re
                    match = re.search(r'Liked by ([\d,]+)', title_attr)
                    if match:
                        stats_data['likes_count_exact'] = int(match.group(1).replace(',', ''))
            
        except Exception as e:
            logging.error(f"Error extracting stats data: {e}")
        
        return stats_data
    
    def _parse_stat_number(self, text: str) -> Optional[int]:
        """
        Parse a statistic number from text like "712K", "183K", "311K".
        
        Args:
            text: Text containing the number
            
        Returns:
            Parsed integer value or None if parsing fails
        """
        try:
            import re
            # Match patterns like "712K", "1.5M", "183K"
            match = re.search(r'([\d,]+(?:\.\d+)?)\s*([KM]?)', text.replace(',', ''))
            if match:
                num = float(match.group(1))
                multiplier = match.group(2)
                
                if multiplier == 'K':
                    num *= 1000
                elif multiplier == 'M':
                    num *= 1000000
                
                return int(num)
        except (ValueError, AttributeError):
            pass
        
        return None
    
    def get_film_ratings_and_stats(self, film_slug: str) -> Dict[str, Any]:
        """
        Get comprehensive ratings and statistics data for a film.
        
        Args:
            film_slug: Film slug (e.g., 'fallen-angels')
            
        Returns:
            Dictionary containing both ratings and stats data
        """
        combined_data = {
            'film_slug': film_slug
        }
        
        try:
            # Get ratings data
            ratings_soup = self.get_film_ratings_soup(film_slug)
            ratings_data = self.extract_film_ratings_data(ratings_soup)
            combined_data.update(ratings_data)
            
            # Get stats data
            stats_soup = self.get_film_stats_soup(film_slug)
            stats_data = self.extract_film_stats_data(stats_soup)
            combined_data.update(stats_data)
            
        except Exception as e:
            logging.error(f"Error getting ratings and stats for {film_slug}: {e}")
        
        return combined_data
    
    def get_all_films_ratings_stats_only(self, username: str, list_name: str,
                                        page_progress_callback: Optional[Callable] = None,
                                        film_progress_callback: Optional[Callable] = None,
                                        max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get only ratings and stats data for all films in a list (optimized version).
        This is much faster than getting full film details.
        
        Args:
            username: Letterboxd username
            list_name: Name of the list
            page_progress_callback: Callback for page progress
            film_progress_callback: Callback for film progress  
            max_workers: Number of workers for parallel processing
            
        Returns:
            List of dictionaries containing basic film info plus ratings and stats
        """
        # Phase 1: Quickly collect all basic film info and slugs
        basic_films = self._collect_all_basic_films_fast(
            username, list_name, page_progress_callback
        )
        
        if not basic_films:
            return []
        
        # Phase 2: Get ratings and stats for all films in parallel
        ratings_stats_films = self._get_ratings_stats_parallel(
            basic_films, film_progress_callback, max_workers
        )
        
        return ratings_stats_films
    
    def _get_ratings_stats_parallel(self, basic_films: List[Dict[str, Any]],
                                   film_progress_callback: Optional[Callable] = None,
                                   max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get ratings and stats data for all films in parallel.
        Optimized for speed by only getting ratings/stats pages.
        
        Args:
            basic_films: List of basic film data with slugs
            film_progress_callback: Callback for progress updates
            max_workers: Number of workers for parallel processing
            
        Returns:
            List of film dictionaries with ratings and stats data
        """
        if not basic_films:
            return []
        
        # Filter films that have slugs
        films_with_slugs = [f for f in basic_films if f.get('film_slug')]
        
        if not films_with_slugs:
            return basic_films
        
        # Determine optimal worker count
        if max_workers is None:
            max_workers = min(8, multiprocessing.cpu_count(), len(films_with_slugs))
        
        enhanced_films = []
        completed = 0
        
        logging.info(f"Getting ratings and stats for {len(films_with_slugs)} films using {max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_film = {
                executor.submit(self._get_film_ratings_stats_safe, film): film
                for film in films_with_slugs
            }
            
            # Process completed tasks
            for future in as_completed(future_to_film):
                original_film = future_to_film[future]
                completed += 1
                
                try:
                    ratings_stats_data = future.result()
                    if ratings_stats_data:
                        # Merge basic data with ratings/stats data
                        enhanced_film = {**original_film, **ratings_stats_data}
                        enhanced_films.append(enhanced_film)
                    else:
                        # Fall back to basic data
                        enhanced_films.append(original_film)
                        
                except Exception as e:
                    logging.error(f"Ratings/stats task failed for {original_film.get('film_slug')}: {e}")
                    enhanced_films.append(original_film)
                
                # Progress callback
                if film_progress_callback:
                    film_progress_callback(completed, len(films_with_slugs),
                                         f"Ratings/Stats: {original_film.get('name', 'Unknown')}")
        
        # Add films without slugs
        films_without_slugs = [f for f in basic_films if not f.get('film_slug')]
        enhanced_films.extend(films_without_slugs)
        
        # Sort by list position if available
        try:
            enhanced_films.sort(key=lambda x: int(x.get('list_position', 999999)))
        except (ValueError, TypeError):
            pass
        
        logging.info(f"Ratings/stats processing complete: {len(enhanced_films)} films")
        return enhanced_films
    
    def _get_film_ratings_stats_safe(self, film: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Thread-safe method to get film ratings and stats with error handling.
        
        Args:
            film: Basic film data containing film_slug
            
        Returns:
            Ratings and stats data or None if failed
        """
        film_slug = film.get('film_slug')
        if not film_slug:
            return None
        
        try:
            # Create thread-local session for thread safety
            session = LetterboxdSession()
            temp_scraper = LetterboxdScraper()
            temp_scraper.session = session
            
            # Get ratings and stats data
            ratings_stats_data = temp_scraper.get_film_ratings_and_stats(film_slug)
            return ratings_stats_data
            
        except Exception as e:
            logging.debug(f"Error getting ratings/stats for {film_slug}: {e}")
            return None

    # ==================== END RATINGS AND STATS METHODS ====================
