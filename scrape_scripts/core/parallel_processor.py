"""
Parallel processing utilities for Letterboxd scraping.
Handles multi-threaded scraping operations efficiently.
"""

import logging
import time
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Callable
from bs4 import BeautifulSoup
from tqdm import tqdm

from core.base_session import BaseSession
from core.letterboxd_utils import FilmDataExtractor, StatsExtractor, ListFilmExtractor, PaginationHelper


class ParallelProcessor:
    """Handles parallel processing for various scraping operations."""
    
    def __init__(self, session_class, base_url: str, headers: Dict[str, str], delay: float = 0.2,
                 film_selectors: Dict[str, Any] = None, stats_selectors: Dict[str, Any] = None):
        self.session_class = session_class
        self.base_url = base_url
        self.headers = headers
        self.delay = delay
        self.film_extractor = FilmDataExtractor(film_selectors)
        self.stats_extractor = StatsExtractor(stats_selectors)
        self.list_extractor = ListFilmExtractor()
        # Thread-local storage for session reuse
        self._thread_local = threading.local()
    
    def _get_thread_session(self):
        """Get or create a session for the current thread. Reuses sessions to avoid
        creating new cloudscraper instances per request (which triggers Cloudflare challenges)."""
        if not hasattr(self._thread_local, 'session'):
            self._thread_local.session = self.session_class()
            self._thread_local.session.configure_for_site()
        return self._thread_local.session
    
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
        """Get ratings and stats data in parallel.
        
        Includes fail-fast: tests the first film before launching full parallel batch.
        If the first film extraction returns no data at all, raises an error.
        """
        films_with_slugs = [f for f in basic_films if f.get('film_slug')]
        
        if not films_with_slugs:
            return basic_films
        
        # Fail-fast: test first film before launching full batch
        first_film = films_with_slugs[0]
        test_result = self._get_ratings_stats_safe(first_film)
        if not test_result or len(test_result) <= 1:  # Only 'film_slug' key = no data extracted
            logging.error(
                f"FAIL-FAST: First film '{first_film.get('film_slug')}' returned no ratings/stats data. "
                f"Result: {test_result}. "
                f"This likely means all /csi/ endpoints are blocked and meta tag fallback also failed. "
                f"Stopping batch to avoid wasting time on {len(films_with_slugs)} films."
            )
            raise RuntimeError(
                f"Ratings/stats extraction failed on first film '{first_film.get('film_slug')}'. "
                f"All extraction methods returned no data. Check network access to Letterboxd /csi/ endpoints."
            )
        
        logging.info(
            f"Fail-fast check passed for '{first_film.get('film_slug')}': "
            f"extracted {len(test_result) - 1} data fields. Proceeding with full batch."
        )
        
        if max_workers is None:
            max_workers = min(4, multiprocessing.cpu_count(), len(films_with_slugs))
        
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
        
        # ── Cleanup pass: re-fetch any films still missing ratings or stats ──
        enhanced_films = self._retry_missing_data(enhanced_films)
        
        # Sort by list position
        try:
            enhanced_films.sort(key=lambda x: int(x.get('list_position', 999999)))
        except (ValueError, TypeError):
            pass
        
        return enhanced_films
    
    # ── Ratings and stats fields expected from each CSI endpoint ──
    RATINGS_FIELDS = ('average_rating', 'total_ratings', 'fans_count')
    STATS_FIELDS = ('watches_count', 'lists_count', 'likes_count')
    
    def _film_missing_ratings(self, film: Dict[str, Any]) -> bool:
        """Check if a film is missing any ratings-summary fields.
        
        Skips fans_count check for films with <2000 watches, as obscure films
        typically don't have fans data and retrying wastes time.
        """
        watches = film.get('watches_count', 0)
        
        # Always check average_rating and total_ratings
        if not film.get('average_rating') or not film.get('total_ratings'):
            return True
        
        # Only check fans_count for films with 2000+ watches
        if watches >= 2000 and not film.get('fans_count'):
            return True
        
        return False
    
    def _film_missing_stats(self, film: Dict[str, Any]) -> bool:
        """Check if a film is missing any stats fields."""
        return any(not film.get(f) for f in self.STATS_FIELDS)
    
    def _retry_missing_data(self, films: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sequential cleanup pass to re-fetch any films still missing ratings or stats.
        
        Runs much slower (one film at a time with generous delays) to avoid
        triggering Cloudflare rate limits. Uses a fresh session.
        """
        # Identify films that need re-fetching
        needs_ratings = [(i, f) for i, f in enumerate(films)
                         if f.get('film_slug') and self._film_missing_ratings(f)]
        needs_stats = [(i, f) for i, f in enumerate(films)
                       if f.get('film_slug') and self._film_missing_stats(f)]
        
        total_retries = len(needs_ratings) + len(needs_stats)
        if total_retries == 0:
            logging.info("Cleanup pass: all films have complete data, nothing to retry.")
            return films
        
        logging.info(
            f"Cleanup pass: {len(needs_ratings)} films missing ratings, "
            f"{len(needs_stats)} missing stats — re-fetching sequentially"
        )
        
        # Use a single fresh session for the sequential retry pass
        session = self.session_class()
        session.configure_for_site()
        
        with tqdm(total=total_retries,
                  desc="🔄 Retrying missing data (slow, 1-by-1)",
                  unit="req",
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                  dynamic_ncols=True) as pbar:
            
            # Re-fetch missing ratings
            for idx, film in needs_ratings:
                slug = film['film_slug']
                pbar.set_postfix_str(f"ratings: {slug}")
                soup = self._fetch_csi_with_retry(
                    session,
                    f"/csi/film/{slug}/ratings-summary/",
                    slug, "ratings-retry",
                    max_retries=5
                )
                if soup:
                    data = self.film_extractor.extract_ratings_data(soup)
                    if data:
                        films[idx].update(data)
                time.sleep(1.5)   # generous delay between sequential retries
                pbar.update(1)
            
            # Re-fetch missing stats
            for idx, film in needs_stats:
                slug = film['film_slug']
                pbar.set_postfix_str(f"stats: {slug}")
                soup = self._fetch_csi_with_retry(
                    session,
                    f"/csi/film/{slug}/stats/",
                    slug, "stats-retry",
                    max_retries=5
                )
                if soup:
                    data = self.stats_extractor.extract_stats_data(soup)
                    if data:
                        films[idx].update(data)
                time.sleep(1.5)
                pbar.update(1)
        
        # Report final status
        still_missing_ratings = sum(1 for f in films if f.get('film_slug') and self._film_missing_ratings(f))
        still_missing_stats = sum(1 for f in films if f.get('film_slug') and self._film_missing_stats(f))
        if still_missing_ratings or still_missing_stats:
            logging.warning(
                f"After cleanup: still {still_missing_ratings} missing ratings, "
                f"{still_missing_stats} missing stats"
            )
        else:
            logging.info("Cleanup pass: all films now have complete data ✓")
        
        return films
    
    def _scrape_single_page(self, page_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape a single page of films (thread-safe)."""
        page = page_info['page']
        page_url = page_info['url']
        start_position = page_info.get('start_position', 1)
        
        try:
            session = self._get_thread_session()
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
            session = self._get_thread_session()
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
    
    def _fetch_csi_with_retry(self, session, url: str, film_slug: str, endpoint_name: str, 
                              max_retries: int = 3) -> Optional[BeautifulSoup]:
        """Fetch a CSI endpoint with retry and exponential backoff for rate limiting.
        
        Returns parsed BeautifulSoup on success, None on failure.
        """
        for attempt in range(max_retries):
            try:
                response = session.session.get(
                    session._build_url(url),
                    timeout=session.timeout
                )
                
                if response.status_code == 200:
                    return BeautifulSoup(response.text, 'html.parser')
                
                if response.status_code in (403, 429, 503):
                    # Rate limited or blocked — back off and retry
                    wait_time = (2 ** attempt) + (attempt * 0.5)
                    logging.debug(
                        f"CSI {endpoint_name} returned {response.status_code} for {film_slug} "
                        f"(attempt {attempt + 1}/{max_retries}), waiting {wait_time:.1f}s"
                    )
                    time.sleep(wait_time)
                    continue
                
                # Other error status — don't retry
                logging.debug(f"CSI {endpoint_name} returned {response.status_code} for {film_slug}")
                return None
                
            except Exception as e:
                wait_time = (2 ** attempt) + (attempt * 0.5)
                logging.debug(
                    f"CSI {endpoint_name} error for {film_slug} (attempt {attempt + 1}/{max_retries}): {e}, "
                    f"waiting {wait_time:.1f}s"
                )
                time.sleep(wait_time)
        
        logging.warning(f"CSI {endpoint_name} failed after {max_retries} retries for {film_slug}")
        return None

    def _get_ratings_stats_safe(self, film: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Thread-safe method to get ratings and stats data.
        
        Reuses a per-thread session to avoid creating new cloudscraper instances
        per request. Includes retry with exponential backoff for rate-limited responses.
        Falls back to meta tag extraction if CSI endpoints fail.
        """
        film_slug = film.get('film_slug')
        if not film_slug:
            return None
        
        try:
            session = self._get_thread_session()
            
            combined_data = {'film_slug': film_slug}
            ratings_ok = False
            stats_ok = False
            
            # Get ratings data from /csi/ endpoint
            ratings_url = f"/csi/film/{film_slug}/ratings-summary/"
            soup = self._fetch_csi_with_retry(session, ratings_url, film_slug, "ratings")
            if soup:
                ratings_data = self.film_extractor.extract_ratings_data(soup)
                if ratings_data:
                    combined_data.update(ratings_data)
                    ratings_ok = True
            
            # Small delay between the two CSI calls to avoid burst detection
            time.sleep(0.3)
            
            # Get stats data from /csi/ endpoint
            stats_url = f"/csi/film/{film_slug}/stats/"
            soup = self._fetch_csi_with_retry(session, stats_url, film_slug, "stats")
            if soup:
                stats_data = self.stats_extractor.extract_stats_data(soup)
                if stats_data:
                    combined_data.update(stats_data)
                    stats_ok = True
            
            # Fallback: if BOTH /csi/ endpoints failed, try main page meta tag
            if not ratings_ok and not stats_ok:
                logging.warning(f"Both /csi/ endpoints failed for {film_slug}, trying main page meta tag fallback")
                try:
                    main_url = f"/film/{film_slug}/"
                    response = session.session.get(
                        session._build_url(main_url),
                        timeout=session.timeout
                    )
                    if response and response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        meta_data = self.film_extractor.extract_ratings_from_meta(soup)
                        if meta_data:
                            combined_data.update(meta_data)
                        else:
                            logging.error(f"ALL extraction methods failed for {film_slug}")
                except Exception as e:
                    logging.error(f"Meta tag fallback also failed for {film_slug}: {e}")
            
            # Add delay after processing each film to pace overall throughput
            time.sleep(self.delay)
            
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
