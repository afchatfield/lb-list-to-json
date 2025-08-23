"""
Parallel processing utilities for Letterboxd scraping.
Handles multi-threaded scraping operations efficiently.
"""

import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Callable
from bs4 import BeautifulSoup

from core.base_session import BaseSession
from core.letterboxd_utils import FilmDataExtractor, StatsExtractor, ListFilmExtractor, PaginationHelper


class ParallelProcessor:
    """Handles parallel processing for various scraping operations."""
    
    def __init__(self, session_class, base_url: str, headers: Dict[str, str], delay: float = 0.2):
        self.session_class = session_class
        self.base_url = base_url
        self.headers = headers
        self.delay = delay
        self.film_extractor = FilmDataExtractor()
        self.stats_extractor = StatsExtractor()
        self.list_extractor = ListFilmExtractor()
    
    def scrape_pages_parallel(self, page_tasks: List[Dict[str, Any]], max_workers: Optional[int] = None,
                             progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Scrape multiple pages in parallel."""
        if max_workers is None:
            max_workers = min(8, multiprocessing.cpu_count())
        
        all_films = []
        completed_pages = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {
                executor.submit(self._scrape_single_page, page_info): page_info['page']
                for page_info in page_tasks
            }
            
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                completed_pages += 1
                
                try:
                    films_from_page = future.result()
                    all_films.extend(films_from_page)
                    
                    if progress_callback:
                        progress_callback(completed_pages, len(page_tasks),
                                        f"Completed page {completed_pages}/{len(page_tasks)}")
                    
                except Exception as e:
                    logging.error(f"Page {page_num} generated an exception: {e}")
        
        # Sort films by their original order
        try:
            all_films.sort(key=lambda x: int(x.get('list_position', 0)))
        except (ValueError, TypeError):
            pass
        
        return all_films
    
    def get_film_details_parallel(self, basic_films: List[Dict[str, Any]], max_workers: Optional[int] = None,
                                 progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Get detailed film information in parallel."""
        films_with_slugs = [f for f in basic_films if f.get('film_slug')]
        
        if not films_with_slugs:
            return basic_films
        
        if max_workers is None:
            max_workers = min(6, multiprocessing.cpu_count(), len(films_with_slugs))
        
        detailed_films = []
        completed = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_film = {
                executor.submit(self._get_film_details_safe, film): film
                for film in films_with_slugs
            }
            
            for future in as_completed(future_to_film):
                original_film = future_to_film[future]
                completed += 1
                
                try:
                    detailed_film = future.result()
                    if detailed_film:
                        # Start with original film data, then update with detailed data
                        # This preserves list-specific fields while adding detailed film info
                        merged_film = original_film.copy()
                        merged_film.update(detailed_film)
                        detailed_films.append(merged_film)
                    else:
                        detailed_films.append(original_film)
                        
                except Exception as e:
                    logging.error(f"Film detail task failed for {original_film.get('film_slug')}: {e}")
                    detailed_films.append(original_film)
                
                if progress_callback:
                    progress_callback(completed, len(films_with_slugs),
                                    f"Details: {original_film.get('name', 'Unknown')}")
        
        # Add films without slugs
        films_without_slugs = [f for f in basic_films if not f.get('film_slug')]
        detailed_films.extend(films_without_slugs)
        
        # Sort by list position
        try:
            detailed_films.sort(key=lambda x: int(x.get('list_position', 999999)))
        except (ValueError, TypeError):
            pass
        
        return detailed_films
    
    def get_ratings_stats_parallel(self, basic_films: List[Dict[str, Any]], max_workers: Optional[int] = None,
                                  progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Get ratings and stats data in parallel."""
        films_with_slugs = [f for f in basic_films if f.get('film_slug')]
        
        if not films_with_slugs:
            return basic_films
        
        if max_workers is None:
            max_workers = min(8, multiprocessing.cpu_count(), len(films_with_slugs))
        
        enhanced_films = []
        completed = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_film = {
                executor.submit(self._get_ratings_stats_safe, film): film
                for film in films_with_slugs
            }
            
            for future in as_completed(future_to_film):
                original_film = future_to_film[future]
                completed += 1
                
                try:
                    ratings_stats_data = future.result()
                    if ratings_stats_data:
                        enhanced_film = {**original_film, **ratings_stats_data}
                        enhanced_films.append(enhanced_film)
                    else:
                        enhanced_films.append(original_film)
                        
                except Exception as e:
                    logging.error(f"Ratings/stats task failed for {original_film.get('film_slug')}: {e}")
                    enhanced_films.append(original_film)
                
                if progress_callback:
                    progress_callback(completed, len(films_with_slugs),
                                    f"Stats: {original_film.get('name', 'Unknown')}")
        
        # Add films without slugs
        films_without_slugs = [f for f in basic_films if not f.get('film_slug')]
        enhanced_films.extend(films_without_slugs)
        
        # Sort by list position
        try:
            enhanced_films.sort(key=lambda x: int(x.get('list_position', 999999)))
        except (ValueError, TypeError):
            pass
        
        return enhanced_films
    
    def _scrape_single_page(self, page_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape a single page of films (thread-safe)."""
        page = page_info['page']
        page_url = page_info['url']
        start_position = page_info.get('start_position', 1)
        
        try:
            session = self.session_class()
            session.configure_for_site()
            response = session.get(page_url)
            
            if response is None:
                logging.error(f"Failed to get response for page {page}")
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            films_on_page = self.list_extractor.extract_films_from_list(soup, start_position)
            
            logging.info(f"Extracted {len(films_on_page)} films from page {page}")
            return films_on_page
            
        except Exception as e:
            logging.error(f"Error scraping page {page}: {e}")
            return []
    
    def _get_film_details_safe(self, film: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Thread-safe method to get film details."""
        film_slug = film.get('film_slug')
        if not film_slug:
            return None
        
        try:
            session = self.session_class()
            session.configure_for_site()
            film_url = f"/film/{film_slug}/"
            
            response = session.get(film_url)
            if response is None:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            detailed_data = self.film_extractor.extract_basic_film_data(soup)
            return detailed_data
            
        except Exception as e:
            logging.debug(f"Error getting details for {film_slug}: {e}")
            return None
    
    def _get_ratings_stats_safe(self, film: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Thread-safe method to get ratings and stats data."""
        film_slug = film.get('film_slug')
        if not film_slug:
            return None
        
        try:
            session = self.session_class()
            session.configure_for_site()
            
            combined_data = {'film_slug': film_slug}
            
            # Get ratings data
            ratings_url = f"/csi/film/{film_slug}/ratings-summary/"
            response = session.get(ratings_url)
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                ratings_data = self.film_extractor.extract_ratings_data(soup)
                combined_data.update(ratings_data)
            
            # Get stats data
            stats_url = f"/csi/film/{film_slug}/stats/"
            response = session.get(stats_url)
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                stats_data = self.stats_extractor.extract_stats_data(soup)
                combined_data.update(stats_data)
            
            return combined_data
            
        except Exception as e:
            logging.debug(f"Error getting ratings/stats for {film_slug}: {e}")
            return None


class BatchProcessor:
    """Handles batch processing operations."""
    
    def __init__(self, session_class, parallel_processor: ParallelProcessor):
        self.session_class = session_class
        self.parallel_processor = parallel_processor
        self.list_extractor = ListFilmExtractor()
        self.pagination_helper = PaginationHelper()
    
    def collect_all_basic_films(self, username: str, list_name: str,
                               progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """Collect all basic film info from all pages sequentially."""
        session = self.session_class()
        
        # Get pagination info
        pagination_url = f"/{username}/list/{list_name}/"
        response = session.get(pagination_url)
        if response is None:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        pagination_info = self.pagination_helper.get_pagination_info(soup)
        total_pages = pagination_info['total_pages']
        
        all_films = []
        global_position = 1
        
        for page in range(1, total_pages + 1):
            try:
                if page == 1:
                    page_url = f"/{username}/list/{list_name}/"
                else:
                    page_url = f"/{username}/list/{list_name}/page/{page}/"
                
                response = session.get(page_url)
                if response is None:
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                films_on_page = self.list_extractor.extract_films_from_list(soup, global_position)
                
                # Update global position
                for film in films_on_page:
                    film['source_page'] = page
                    film['source_list'] = f"{username}/{list_name}"
                    global_position += 1
                
                all_films.extend(films_on_page)
                
                if progress_callback:
                    progress_callback(page, total_pages, f"Collected {len(films_on_page)} films")
                
            except Exception as e:
                logging.error(f"Error collecting films from page {page}: {e}")
                continue
        
        logging.info(f"Collected {len(all_films)} films from {total_pages} pages")
        return all_films
    
    def prepare_page_tasks(self, username: str, list_name: str, total_pages: int) -> List[Dict[str, Any]]:
        """Prepare page tasks for parallel processing."""
        page_tasks = []
        films_per_page = 100  # Standard Letterboxd page size
        
        for page in range(1, total_pages + 1):
            page_url = self.pagination_helper.build_page_url(username, list_name, page)
            start_position = ((page - 1) * films_per_page) + 1
            
            page_tasks.append({
                'page': page,
                'username': username,
                'list_name': list_name,
                'url': page_url,
                'start_position': start_position
            })
        
        return page_tasks
