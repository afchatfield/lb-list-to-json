"""
Utility classes and functions for Letterboxd scraping operations.
Contains common functionality to reduce code duplication.
"""

import re
import logging
from typing import Optional, Dict, Any, List
from bs4 import BeautifulSoup


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
        self.selectors = selectors or {
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


class StatsExtractor(DataExtractor):
    """Specialized extractor for film statistics."""
    
    SELECTORS = {
        'stats_container': '.production-statistic-list',
        'watches_stat': '.production-statistic.-watches',
        'lists_stat': '.production-statistic.-lists',
        'likes_stat': '.production-statistic.-likes',
        'label': '.label',
        'link': 'a'
    }
    
    def extract_stats_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract statistical information from film stats page."""
        stats_data = {}
        
        stats_container = soup.select_one(self.SELECTORS['stats_container'])
        if not stats_container:
            return stats_data
        
        # Extract each type of statistic
        stats_data.update(self._extract_single_stat(stats_container, 'watches', 'Watched by'))
        stats_data.update(self._extract_single_stat(stats_container, 'lists', 'Appears in'))
        stats_data.update(self._extract_single_stat(stats_container, 'likes', 'Liked by'))
        
        return stats_data
    
    def _extract_single_stat(self, container: BeautifulSoup, stat_type: str, title_pattern: str) -> Dict[str, Any]:
        """Extract a single type of statistic."""
        result = {}
        stat_elem = container.select_one(f'.production-statistic.-{stat_type}')
        
        if stat_elem:
            # Get from label text
            label = stat_elem.select_one(self.SELECTORS['label'])
            if label:
                label_text = label.get_text(strip=True)
                count = self.extract_number_from_text(label_text)
                if count:
                    result[f'{stat_type}_count'] = count
            
            # Get exact count from title attribute
            link = stat_elem.select_one(self.SELECTORS['link'])
            if link:
                title_attr = link.get('title', '')
                exact_count = self.extract_count_from_title(title_attr, f'{title_pattern} ([\\d,]+)')
                if exact_count:
                    result[f'{stat_type}_count_exact'] = exact_count
        
        return result


class PaginationHelper:
    """Helper class for handling pagination in lists."""
    
    @staticmethod
    def get_pagination_info(soup: BeautifulSoup) -> Dict[str, int]:
        """Extract pagination information from a list page."""
        pagination = soup.find('div', class_='paginate-pages')
        total_pages = 1
        
        if pagination:
            page_links = pagination.find_all('a')
            if page_links:
                last_page_link = page_links[-1]
                if 'page' in last_page_link.get('href', ''):
                    try:
                        total_pages = int(last_page_link.get('href').split('page/')[1].rstrip('/'))
                    except (ValueError, IndexError):
                        total_pages = 1
        
        # Count films on current page
        films_on_page = len(soup.select('.poster-container'))
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
    
    def extract_films_from_list(self, soup: BeautifulSoup, start_position: int = 1) -> List[Dict[str, Any]]:
        """Extract film data from a list page."""
        films = []
        current_position = start_position
        
        # Try multiple selectors for compatibility with different website versions
        # First try the new structure (data-item-slug)
        film_posters = soup.select('div[data-item-slug]')
        
        # Fallback to old structure (data-film-slug) 
        if not film_posters:
            film_posters = soup.select('div[data-film-slug]')
        
        # Additional fallback using data-film-id
        if not film_posters:
            film_posters = soup.select('div[data-film-id]')
        
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
        # Try both new and old attribute names for film slug
        film_slug = poster.get('data-item-slug') or poster.get('data-film-slug')
        if not film_slug:
            return None

        # Try both new and old attribute names for film name
        film_name = poster.get('data-item-name')
        if not film_name:
            # Fallback to img alt attribute
            img = poster.select_one('img')
            if img:
                film_name = img.get('alt', '').strip()
        
        # Additional fallback to data attributes
        if not film_name:
            film_name = poster.get('data-film-name', 'Unknown')

        # Get owner rating from parent list item
        list_item = poster.find_parent('li')
        owner_rating = None
        if list_item:
            owner_rating = list_item.get('data-owner-rating')

        # Try both new and old attribute names for target link
        target_link = poster.get('data-item-link') or poster.get('data-target-link')

        return {
            'film_slug': film_slug,
            'name': film_name,
            'film_id': poster.get('data-film-id'),
            'target_link': target_link,
            'list_position': str(position),
            'owner_rating': owner_rating
        }
