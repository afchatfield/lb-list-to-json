"""
Utility classes and functions for Letterboxd scraping operations.
Contains common functionality to reduce code duplication.
"""

import re
import logging
import json
import os
from typing import Optional, Dict, Any, List
from bs4 import BeautifulSoup


def load_letterboxd_selectors() -> Dict[str, Any]:
    """
    Load Letterboxd selectors from the JSON configuration file.
    
    Returns:
        Dictionary containing all selector configurations
    """
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'configs',
        'letterboxd_selectors.json'
    )
    
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Selectors config file not found: {config_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in selectors config: {e}")
        return {}


def get_selector_category(category: str) -> Dict[str, str]:
    """
    Get a specific category of selectors.
    
    Args:
        category: Category name (e.g., 'film_list', 'film_page', 'pagination', 'attributes')
    
    Returns:
        Dictionary of selectors for the category
    """
    selectors = load_letterboxd_selectors()
    return selectors.get(category, {})


class DataExtractor:
    """Utility class for extracting data from BeautifulSoup elements."""
    
    @staticmethod
    def extract_text(soup: BeautifulSoup, selector: str, default: str = "") -> str:
        """Extract text from an element using CSS selector."""
        try:
            element = soup.select_one(selector)
            return element.get_text(strip=True) if element else default
        except Exception as e:
            logging.debug(f"Error extracting text with selector '{selector}': {e}")
            return default
    
    @staticmethod
    def extract_text_list(soup: BeautifulSoup, selector: str) -> List[str]:
        """Extract a list of text values from elements matching a selector."""
        try:
            elements = soup.select(selector)
            return [elem.get_text(strip=True) for elem in elements if elem.get_text(strip=True)]
        except Exception as e:
            logging.debug(f"Error extracting text list with selector '{selector}': {e}")
            return []
    
    @staticmethod
    def extract_attribute(soup: BeautifulSoup, selector: str, attribute: str, default: Any = None) -> Any:
        """Extract an attribute value from an element."""
        try:
            element = soup.select_one(selector)
            return element.get(attribute, default) if element else default
        except Exception as e:
            logging.debug(f"Error extracting attribute '{attribute}' with selector '{selector}': {e}")
            return default
    
    @staticmethod
    def extract_number_from_text(text: str) -> Optional[int]:
        """Extract a number from text like '712K', '1.5M', '183K'."""
        try:
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
    
    @staticmethod
    def extract_rating_from_title(title_attr: str) -> Optional[float]:
        """Extract rating from title attribute."""
        try:
            match = re.search(r'average of ([\d.]+)', title_attr, re.IGNORECASE)
            if match:
                return float(match.group(1))
        except (ValueError, AttributeError):
            pass
        return None
    
    @staticmethod
    def extract_count_from_title(title_attr: str, pattern: str) -> Optional[int]:
        """Extract count from title attribute using a pattern."""
        try:
            match = re.search(pattern, title_attr)
            if match:
                return int(match.group(1).replace(',', ''))
        except (ValueError, AttributeError):
            pass
        return None


class FilmDataExtractor(DataExtractor):
    """Specialized data extractor for film information."""
    
    def __init__(self, selectors: Dict[str, Any] = None):
        """Initialize with selectors configuration."""
        # Default selectors as fallback - these should match letterboxd_selectors.json
        default_selectors = {
            'title': 'h1.headline-1 .name',
            'year': '.releasedate a',
            'director': '.credits .prettify',
            'genres': '#tab-genres .text-slug',
            'countries': '#tab-details .text-sluglist a[href*="/films/country/"]',
            'primary_language': "#tab-details .text-sluglist a[href*='/films/language/']:first-of-type",
            'other_languages': "#tab-details .text-sluglist a[href*='/films/language/']",
            'studios': '#tab-details .text-sluglist a[href*="/studio/"]',
            'cast': '.cast-list .text-slug',
            'runtime': 'p.text-link',
            'rating_section': 'section.ratings-histogram-chart',
            'average_rating': '.average-rating .display-rating',
            'fans_link': 'a[href*="/fans/"]',
            'histogram_bars': '.rating-histogram-bar a'
        }
        
        # Use provided selectors or defaults
        self.selectors = selectors if selectors else default_selectors
    
    def extract_basic_film_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract basic film information efficiently."""
        film_data = {}
        
        # Title
        film_data['title'] = self.extract_text(soup, self.selectors['title'])
        
        # Year
        year_text = self.extract_text(soup, self.selectors['year'])
        if year_text.isdigit():
            film_data['year'] = int(year_text)
        
        # Director
        director = self.extract_text(soup, self.selectors['director'])
        film_data['director'] = director
        
        # Extract and separate genres from themes
        all_genres = self.extract_text_list(soup, self.selectors['genres'])
        genres, themes = self._separate_genres_and_themes(all_genres)
        film_data['genres'] = genres
        film_data['themes'] = themes
        film_data['countries'] = self.extract_text_list(soup, self.selectors['countries'])

        primary_language = self.extract_text(soup, self.selectors.get('primary_language', ''))
        other_languages = self.extract_text_list(soup, self.selectors.get('other_languages', ''))
        
        film_data['primary_language'] = primary_language
        if primary_language and other_languages:
            film_data['other_languages'] = [lang for lang in other_languages if lang != primary_language]
        else:
            film_data['other_languages'] = other_languages
        
        film_data['studios'] = self.extract_text_list(soup, self.selectors.get('studios', ''))
        film_data['cast'] = self.extract_text_list(soup, self.selectors['cast'])[:10]  # Limit for speed
        
        # Runtime
        runtime_text = self.extract_text(soup, self.selectors['runtime'])
        if 'min' in runtime_text:
            try:
                film_data['runtime'] = int(runtime_text.split()[0])
            except (ValueError, IndexError):
                pass
        
        return film_data
    
    def extract_ratings_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract detailed ratings information."""
        ratings_data = {}
        
        ratings_section = soup.select_one(self.selectors['rating_section'])
        if not ratings_section:
            return ratings_data
        
        # Average rating and total count
        avg_rating_elem = ratings_section.select_one(self.selectors['average_rating'])
        if avg_rating_elem:
            title_attr = avg_rating_elem.get('title', '')
            if title_attr:
                # Extract precise rating
                rating = self.extract_rating_from_title(title_attr)
                if rating:
                    ratings_data['average_rating'] = rating
                
                # Extract total ratings count
                count = self.extract_count_from_title(title_attr, r'based on ([\d,]+)')
                if count:
                    ratings_data['total_ratings'] = count
        
        # Fan count
        fans_link = ratings_section.select_one(self.selectors['fans_link'])
        if fans_link:
            fans_text = fans_link.get_text(strip=True)
            fans_count = self.extract_number_from_text(fans_text)
            if fans_count:
                ratings_data['fans_count'] = fans_count
        
        # Ratings breakdown
        breakdown = self._extract_ratings_breakdown(ratings_section)
        if breakdown:
            ratings_data['ratings_breakdown'] = breakdown
        
        return ratings_data
    
    def _extract_ratings_breakdown(self, ratings_section: BeautifulSoup) -> Dict[str, Any]:
        """Extract individual star ratings breakdown."""
        breakdown = {}
        histogram_bars = ratings_section.select(self.selectors['histogram_bars'])
        
        for bar in histogram_bars:
            title_attr = bar.get('title', '')
            if title_attr:
                match = re.search(r'([\d,]+)\s*(★+(?:½)?)\s*ratings\s*\((\d+)%\)', title_attr)
                if match:
                    count = int(match.group(1).replace(',', ''))
                    stars = match.group(2)
                    percentage = int(match.group(3))
                    
                    # Convert stars to numeric value
                    star_value = len(stars.replace('½', ''))
                    if '½' in stars:
                        star_value -= 0.5
                    
                    breakdown[f'stars_{star_value}'] = {
                        'count': count,
                        'percentage': percentage
                    }
        
        return breakdown

    def _separate_genres_and_themes(self, all_genres: List[str]) -> tuple[List[str], List[str]]:
        """Separate basic genres from thematic descriptions."""
        # Standard Letterboxd genres (basic film categories)
        BASIC_GENRES = {
            'Action', 'Adventure', 'Animation', 'Comedy', 'Crime', 'Documentary', 
            'Drama', 'Family', 'Fantasy', 'History', 'Horror', 'Music', 'Mystery', 
            'Romance', 'Science Fiction', 'Thriller', 'TV Movie', 'War', 'Western'
        }
        
        genres = []
        themes = []
        
        for item in all_genres:
            # Skip "Show All…" entries
            if item.startswith('Show All'):
                continue
                
            # Check if it's a basic genre (case-insensitive)
            if any(item.lower() == genre.lower() for genre in BASIC_GENRES):
                genres.append(item)
            else:
                themes.append(item)
        
        return genres, themes
    
    def extract_ratings_from_meta(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Fallback: extract average rating from main film page twitter:data2 meta tag.
        
        This works when /csi/ endpoints are unavailable. Only provides average_rating,
        not total_ratings, fans_count, or ratings_breakdown.
        
        Args:
            soup: BeautifulSoup of the main film page (/film/{slug}/)
            
        Returns:
            Dict with 'average_rating' if found, empty dict otherwise
        """
        ratings_data = {}
        
        # Try twitter:data2 meta tag: <meta name="twitter:data2" content="4.13 out of 5">
        meta_selector = self.selectors.get('average_rating_meta', "meta[name='twitter:data2']")
        meta_attr = self.selectors.get('average_rating_meta_attr', 'content')
        meta_tag = soup.select_one(meta_selector)
        
        if meta_tag:
            content = meta_tag.get(meta_attr, '')
            if content:
                match = re.search(r'([\d.]+)\s+out of\s+5', content)
                if match:
                    try:
                        ratings_data['average_rating'] = float(match.group(1))
                    except ValueError:
                        pass
        
        if ratings_data:
            logging.debug(f"Extracted average_rating={ratings_data.get('average_rating')} from meta tag")
        else:
            logging.debug("No average rating found in meta tags")
        
        return ratings_data


class StatsExtractor(DataExtractor):
    """Specialized extractor for film statistics."""
    
    DEFAULT_SELECTORS = {
        'container': '.production-statistic-list',
        'watches': '.production-statistic.-watches',
        'lists': '.production-statistic.-lists',
        'likes': '.production-statistic.-likes',
        'label': '.label',
        'link': 'a',
        'watches_title_pattern': 'Watched by ([\\d,]+)',
        'lists_title_pattern': 'Appears in ([\\d,]+)',
        'likes_title_pattern': 'Liked by ([\\d,]+)',
    }
    
    def __init__(self, selectors: Dict[str, Any] = None):
        """Initialize with optional selectors from config."""
        self.selectors = selectors if selectors else self.DEFAULT_SELECTORS
    
    def extract_stats_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract statistical information from film stats page."""
        stats_data = {}
        
        stats_container = soup.select_one(self.selectors.get('container', '.production-statistic-list'))
        if not stats_container:
            return stats_data
        
        # Extract each type of statistic
        stats_data.update(self._extract_single_stat(
            stats_container, 'watches',
            self.selectors.get('watches_title_pattern', 'Watched by ([\\d,]+)')))
        stats_data.update(self._extract_single_stat(
            stats_container, 'lists',
            self.selectors.get('lists_title_pattern', 'Appears in ([\\d,]+)')))
        stats_data.update(self._extract_single_stat(
            stats_container, 'likes',
            self.selectors.get('likes_title_pattern', 'Liked by ([\\d,]+)')))
        
        return stats_data
    
    def _extract_single_stat(self, container: BeautifulSoup, stat_type: str, title_pattern: str) -> Dict[str, Any]:
        """Extract a single type of statistic."""
        result = {}
        stat_selector = self.selectors.get(stat_type, f'.production-statistic.-{stat_type}')
        stat_elem = container.select_one(stat_selector)
        
        if stat_elem:
            # Get from label text (abbreviated, e.g. "712K")
            label_selector = self.selectors.get('label', '.label')
            label = stat_elem.select_one(label_selector)
            if label:
                label_text = label.get_text(strip=True)
                count = self.extract_number_from_text(label_text)
                if count:
                    result[f'{stat_type}_count'] = count
            
            # Get exact count from title attribute (e.g. "Watched by 711,602 members")
            link_selector = self.selectors.get('link', 'a')
            link = stat_elem.select_one(link_selector)
            if link:
                title_attr = link.get('title', '')
                exact_count = self.extract_count_from_title(title_attr, title_pattern)
                if exact_count:
                    result[f'{stat_type}_count_exact'] = exact_count
        
        return result


class PaginationHelper:
    """Helper class for handling pagination in lists."""
    
    def __init__(self, selectors: Dict[str, Any] = None):
        """Initialize with selectors configuration."""
        # Default selectors as fallback - these should match letterboxd_selectors.json
        default_selectors = {
            'pagination': {
                'pagination_container': '.pagination',
                'page_links': '.pagination li a',
                'last_page': '.pagination li:last-child a'
            },
            'film_list': {
                'poster_container': '.poster-container'
            }
        }
        
        # Use provided selectors or defaults
        self.selectors = selectors if selectors else default_selectors
    
    def get_pagination_info(self, soup: BeautifulSoup) -> Dict[str, int]:
        """Extract pagination information from a list page."""
        pagination_selector = self.selectors.get('pagination', {}).get('pagination_container', '.pagination')
        pagination = soup.select_one(pagination_selector.replace('.pagination', 'div.paginate-pages'))
        total_pages = 1
        
        if pagination:
            page_links_selector = self.selectors.get('pagination', {}).get('page_links', '.pagination li a')
            page_links = pagination.select('a')  # Use direct 'a' since we already have the pagination container
            if page_links:
                last_page_link = page_links[-1]
                if 'page' in last_page_link.get('href', ''):
                    try:
                        total_pages = int(last_page_link.get('href').split('page/')[1].rstrip('/'))
                    except (ValueError, IndexError):
                        total_pages = 1
        
        # Count films on current page using configurable selector
        poster_selector = self.selectors.get('film_list', {}).get('poster_container', '.poster-container')
        films_on_page = len(soup.select(poster_selector))
        total_films_estimate = films_on_page * total_pages
        
        return {
            'total_pages': total_pages,
            'total_films_estimate': total_films_estimate,
            'films_per_page': films_on_page
        }
    
    @staticmethod
    def build_page_url(username: str, list_name: str, page: int) -> str:
        """Build URL for a specific page of a list."""
        if page == 1:
            return f"/{username}/list/{list_name}/"
        else:
            return f"/{username}/list/{list_name}/page/{page}/"


class ListFilmExtractor(DataExtractor):
    """Specialized extractor for films from list pages."""
    
    def __init__(self, selectors: Dict[str, Any] = None):
        """Initialize with selectors configuration."""
        # Default selectors as fallback - these should match letterboxd_selectors.json
        default_selectors = {
            'film_list': {
                'poster_container': '.poster-container',
                'data_item_slug': 'div[data-item-slug]',
                'data_film_slug': 'div[data-film-slug]',
                'data_film_id': 'div[data-film-id]',
                'film_img': 'img'
            },
            'attributes': {
                'data_item_slug': 'data-item-slug',
                'data_film_slug': 'data-film-slug',
                'data_film_id': 'data-film-id',
                'data_item_name': 'data-item-name',
                'data_film_name': 'data-film-name',
                'data_owner_rating': 'data-owner-rating',
                'data_target_link': 'data-target-link',
                'data_item_link': 'data-item-link',
                'alt': 'alt'
            }
        }
        
        # Use provided selectors or defaults
        self.selectors = selectors if selectors else default_selectors
    
    def extract_films_from_list(self, soup: BeautifulSoup, start_position: int = 1) -> List[Dict[str, Any]]:
        """Extract film data from a list page."""
        films = []
        current_position = start_position
        
        # Get selectors from config
        film_list_selectors = self.selectors.get('film_list', {})
        
        # Try multiple selectors for compatibility with different website versions
        # First try the new structure (data-item-slug)
        film_posters = soup.select(film_list_selectors.get('data_item_slug', 'div[data-item-slug]'))
        
        # Fallback to old structure (data-film-slug) 
        if not film_posters:
            film_posters = soup.select(film_list_selectors.get('data_film_slug', 'div[data-film-slug]'))
        
        # Additional fallback using data-film-id
        if not film_posters:
            film_posters = soup.select(film_list_selectors.get('data_film_id', 'div[data-film-id]'))
        
        for poster in film_posters:
            try:
                film_data = self._extract_single_film_from_poster(poster, current_position)
                if film_data:
                    films.append(film_data)
                    current_position += 1
            except Exception as e:
                logging.debug(f"Error extracting film data: {e}")
                continue
        
        return films
    
    def _extract_single_film_from_poster(self, poster: BeautifulSoup, position: int) -> Optional[Dict[str, Any]]:
        """Extract data from a single film poster element."""
        # Get attribute names from config
        attrs = self.selectors.get('attributes', {})
        
        # Try both new and old attribute names for film slug
        film_slug = (poster.get(attrs.get('data_item_slug', 'data-item-slug')) or 
                    poster.get(attrs.get('data_film_slug', 'data-film-slug')))
        if not film_slug:
            return None

        # Try both new and old attribute names for film name
        film_name = poster.get(attrs.get('data_item_name', 'data-item-name'))
        if not film_name:
            # Fallback to img alt attribute
            img_selector = self.selectors.get('film_list', {}).get('film_img', 'img')
            img = poster.select_one(img_selector)
            if img:
                film_name = img.get(attrs.get('alt', 'alt'), '').strip()
        
        # Additional fallback to data attributes
        if not film_name:
            film_name = poster.get(attrs.get('data_film_name', 'data-film-name'), 'Unknown')

        # Get owner rating from parent list item
        list_item = poster.find_parent('li')
        owner_rating = None
        if list_item:
            owner_rating = list_item.get(attrs.get('data_owner_rating', 'data-owner-rating'))

        # Try both new and old attribute names for target link
        target_link = (poster.get(attrs.get('data_item_link', 'data-item-link')) or 
                      poster.get(attrs.get('data_target_link', 'data-target-link')))

        return {
            'film_slug': film_slug,
            'name': film_name,
            'film_id': poster.get(attrs.get('data_film_id', 'data-film-id')),
            'target_link': target_link,
            'list_position': str(position),
            'owner_rating': owner_rating
        }


class BrowseFilmExtractor(DataExtractor):
    """Extractor for films from Letterboxd browse/AJAX pages (country/language/genre filters).
    
    These pages use a different HTML structure than list pages:
    - Films are in <li class="posteritem"> elements
    - Data attributes are on nested <div class="react-component"> elements
    - Rating is on the <li> element itself as data-average-rating
    """
    
    DEFAULT_SELECTORS = {
        'ajax_base': '/films/ajax',
        'film_container': 'li.posteritem',
        'film_data': '.react-component',
        'attributes': {
            'name': 'data-item-name',
            'slug': 'data-item-slug',
            'film_id': 'data-film-id',
            'link': 'data-item-link',
            'rating': 'data-average-rating',
        },
        'pagination': {
            'next': '.paginate-nextprev .next',
            'pages': '.paginate-pages li a',
        },
    }
    
    def __init__(self, selectors: Dict[str, Any] = None):
        self.selectors = selectors if selectors else self.DEFAULT_SELECTORS
    
    def extract_films_from_browse(self, soup: BeautifulSoup, start_rank: int = 1) -> List[Dict[str, Any]]:
        """Extract film data from a browse/AJAX page response.
        
        Args:
            soup: Parsed HTML of the AJAX response
            start_rank: The rank number for the first film on this page
                        (e.g. page 2 with 72 per page → start_rank=73)
        """
        films = []
        container_sel = self.selectors.get('film_container', 'li.posteritem')
        data_sel = self.selectors.get('film_data', '.react-component')
        attrs = self.selectors.get('attributes', self.DEFAULT_SELECTORS['attributes'])
        
        items = soup.select(container_sel)
        for idx, item in enumerate(items):
            rc = item.select_one(data_sel)
            if not rc:
                continue
            
            slug = rc.get(attrs.get('slug', 'data-item-slug'), '')
            if not slug:
                continue
            
            name_raw = rc.get(attrs.get('name', 'data-item-name'), '')
            film_id = rc.get(attrs.get('film_id', 'data-film-id'), '')
            link = rc.get(attrs.get('link', 'data-item-link'), '')
            rating_str = item.get(attrs.get('rating', 'data-average-rating'), '')
            
            # Parse name and year from "Title (Year)" format
            name = name_raw
            year = None
            if name_raw and name_raw.endswith(')'):
                import re
                match = re.match(r'^(.+?)\s*\((\d{4})\)$', name_raw)
                if match:
                    name = match.group(1).strip()
                    year = int(match.group(2))
            
            rating = None
            if rating_str:
                try:
                    rating = float(rating_str)
                except ValueError:
                    pass
            
            films.append({
                'browse_rank': start_rank + idx,
                'film_slug': slug,
                'name': name,
                'name_with_year': name_raw,
                'film_id': film_id,
                'year': year,
                'average_rating': rating,
                'target_link': link,
            })
        
        return films
    
    def has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there's a next page in pagination."""
        next_sel = self.selectors.get('pagination', {}).get('next', '.paginate-nextprev .next')
        return soup.select_one(next_sel) is not None
    
    def get_total_pages(self, soup: BeautifulSoup) -> Optional[int]:
        """Get total page count from pagination links.
        
        Returns:
            Total number of pages, or None if total is unknown
            (browse AJAX pages only have next/prev, not numbered page links).
        """
        pages_sel = self.selectors.get('pagination', {}).get('pages', '.paginate-pages li a')
        page_links = soup.select(pages_sel)
        if not page_links:
            # No numbered page links — common for browse/AJAX pages.
            # Return None to signal unknown total (caller should use has_next_page instead).
            return None
        # Get highest page number from link text
        max_page = 1
        for link in page_links:
            text = link.get_text(strip=True)
            if text.isdigit():
                max_page = max(max_page, int(text))
        return max_page
    
    @staticmethod
    def build_ajax_url(production_country: str = None, rating_country: str = None, 
                      language: str = None, sort: str = 'rating', page: int = 1) -> str:
        """Build the AJAX endpoint URL for browse pages.
        
        Uses the /in/{rating_country}/ path segment to get Letterboxd's weighted
        local preference ranking (not just raw average rating).
        
        Format: /films/ajax/by/{sort}/in/{rating_country}/country/{production_country}/language/{language}/page/{page}/
        
        Args:
            production_country: Country slug for films produced in (e.g. 'france', 'italy', 'japan'). Optional.
            rating_country: Country slug for weighting ratings by that country's preferences. Optional.
            language: Language slug (e.g. 'french', 'italian'). Optional.
            sort: Sort method ('rating', 'popular', etc.)
            page: Page number (1-indexed)
        """
        parts = ['/films/ajax']
        parts.append(f'/by/{sort}')
        if rating_country:
            parts.append(f'/in/{rating_country}')
        if production_country:
            parts.append(f'/country/{production_country}')
        if language:
            parts.append(f'/language/{language}')
        if page > 1:
            parts.append(f'/page/{page}')
        parts.append('/')
        return ''.join(parts)
