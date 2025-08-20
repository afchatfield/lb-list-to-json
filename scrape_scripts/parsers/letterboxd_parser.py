"""
Letterboxd-specific parser for converting scraped data to pandas DataFrames.
Handles films, lists, and user data with Letterboxd-specific cleaning and validation.
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

import sys
import os
# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.base_parser import BaseParser

logger = logging.getLogger(__name__)


class LetterboxdParser(BaseParser):
    """
    Parser for Letterboxd data with film-specific cleaning and validation.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Add Letterboxd-specific cleaners
        self.cleaners.update({
            'rating': self._clean_rating,
            'runtime': self._clean_runtime,
            'genre_list': self._clean_genre_list,
            'cast_list': self._clean_cast_list,
            'country_list': self._clean_country_list,
            'film_id': self._clean_film_id,
            'list_position': self._clean_list_position,
            'stat_count': self._clean_stat_count,
            'ratings_breakdown': self._clean_ratings_breakdown
        })
    
    def get_cleaning_rules(self) -> Dict[str, str]:
        """Return column cleaning rules for Letterboxd data."""
        return {
            'title': 'text',
            'original_title': 'text',
            'director': 'text',
            'tagline': 'text',
            'synopsis': 'text',
            'name': 'text',
            'year': 'year',
            'target_link': 'url',
            'film_id': 'film_id',
            'film_slug': 'text',
            'list_position': 'list_position',
            'owner_rating': 'rating',
            'runtime': 'runtime',
            'genres': 'genre_list',
            'cast': 'cast_list',
            'countries': 'country_list',
            # New ratings and stats fields
            'average_rating': 'rating',
            'total_ratings': 'stat_count',
            'fans_count': 'stat_count',
            'watches_count': 'stat_count',
            'watches_count_exact': 'stat_count',
            'lists_count': 'stat_count',
            'lists_count_exact': 'stat_count',
            'likes_count': 'stat_count',
            'likes_count_exact': 'stat_count',
            'ratings_breakdown': 'ratings_breakdown'
        }
    
    def get_required_columns(self) -> List[str]:
        """Return required columns for film data."""
        return ['name', 'film_slug']  # Minimal requirements
    
    def validate_content(self, df: pd.DataFrame) -> None:
        """Perform Letterboxd-specific content validation."""
        # Validate film slugs
        if 'film_slug' in df.columns:
            invalid_slugs = df[df['film_slug'].str.contains(r'[^a-z0-9\-]', na=False)]
            if not invalid_slugs.empty:
                logger.warning(f"Found {len(invalid_slugs)} films with invalid slugs")
        
        # Validate years
        if 'year' in df.columns:
            current_year = datetime.now().year
            invalid_years = df[
                (df['year'] < 1800) | (df['year'] > current_year + 10)
            ]['year'].dropna()
            if not invalid_years.empty:
                logger.warning(f"Found {len(invalid_years)} films with questionable years")
        
        # Validate ratings
        if 'owner_rating' in df.columns:
            invalid_ratings = df[
                (df['owner_rating'] < 0) | (df['owner_rating'] > 10)
            ]['owner_rating'].dropna()
            if not invalid_ratings.empty:
                logger.warning(f"Found {len(invalid_ratings)} films with invalid ratings")
    
    def _clean_rating(self, rating: Union[str, int, float]) -> Optional[float]:
        """Clean Letterboxd rating (0-10 scale)."""
        if pd.isna(rating) or rating == '':
            return None
        
        if isinstance(rating, str):
            # Handle star ratings (★★★★☆ format)
            if '★' in rating:
                stars = rating.count('★')
                half_stars = rating.count('½')
                return stars + (half_stars * 0.5)
            
            # Extract numeric rating
            rating_match = re.search(r'(\d+(?:\.\d+)?)', rating)
            if rating_match:
                rating = float(rating_match.group(1))
            else:
                return None
        
        try:
            rating = float(rating)
            # Letterboxd uses 0-10 scale
            if 0 <= rating <= 10:
                return rating
            # Convert from 5-star scale to 10-point scale
            elif 0 <= rating <= 5:
                return rating * 2
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    def _clean_runtime(self, runtime: str) -> Optional[int]:
        """Extract runtime in minutes from text."""
        if not isinstance(runtime, str) or not runtime:
            return None
        
        # Look for patterns like "120 mins", "2h 30m", etc.
        hours_match = re.search(r'(\d+)h', runtime)
        mins_match = re.search(r'(\d+)m', runtime)
        
        total_mins = 0
        
        if hours_match:
            total_mins += int(hours_match.group(1)) * 60
        
        if mins_match:
            total_mins += int(mins_match.group(1))
        
        # If no hours/mins format, look for just minutes
        if total_mins == 0:
            mins_only = re.search(r'(\d+)\s*mins?', runtime)
            if mins_only:
                total_mins = int(mins_only.group(1))
        
        return total_mins if total_mins > 0 else None
    
    def _clean_genre_list(self, genres: Union[str, List[str]]) -> List[str]:
        """Clean and standardize genre list."""
        if isinstance(genres, str):
            if not genres or genres == 'N/A':
                return []
            
            # Split by common delimiters
            genres = re.split(r'[,;·•]', genres)
        
        if not isinstance(genres, list):
            return []
        
        cleaned_genres = []
        for genre in genres:
            if isinstance(genre, str):
                genre = genre.strip()
                if genre and genre != 'N/A':
                    # Standardize common genre names
                    genre = self._standardize_genre(genre)
                    cleaned_genres.append(genre)
        
        return cleaned_genres
    
    def _clean_cast_list(self, cast: Union[str, List[str]]) -> List[str]:
        """Clean and standardize cast list."""
        if isinstance(cast, str):
            if not cast or cast == 'N/A':
                return []
            
            # Split by common delimiters
            cast = re.split(r'[,;]', cast)
        
        if not isinstance(cast, list):
            return []
        
        cleaned_cast = []
        for actor in cast:
            if isinstance(actor, str):
                actor = self._clean_text(actor)
                if actor and actor != 'N/A':
                    cleaned_cast.append(actor)
        
        return cleaned_cast
    
    def _clean_country_list(self, countries: Union[str, List[str]]) -> List[str]:
        """Clean and standardize country list."""
        if isinstance(countries, str):
            if not countries or countries == 'N/A':
                return []
            
            # Split by common delimiters
            countries = re.split(r'[,;]', countries)
        
        if not isinstance(countries, list):
            return []
        
        cleaned_countries = []
        for country in countries:
            if isinstance(country, str):
                country = country.strip()
                if country and country != 'N/A':
                    # Standardize common country names
                    country = self._standardize_country(country)
                    cleaned_countries.append(country)
        
        return cleaned_countries
    
    def _clean_film_id(self, film_id: Union[str, int]) -> Optional[int]:
        """Clean and validate film ID."""
        if pd.isna(film_id) or film_id == '':
            return None
        
        try:
            return int(film_id)
        except (ValueError, TypeError):
            # Extract numbers from string
            if isinstance(film_id, str):
                numbers = re.findall(r'\d+', film_id)
                if numbers:
                    return int(numbers[0])
            return None
    
    def _clean_list_position(self, position: Union[str, int]) -> Optional[int]:
        """Clean and validate list position."""
        if pd.isna(position) or position == '':
            return None
        
        try:
            return int(position)
        except (ValueError, TypeError):
            # Extract numbers from string
            if isinstance(position, str):
                numbers = re.findall(r'\d+', position)
                if numbers:
                    return int(numbers[0])
            return None
    
    def _clean_stat_count(self, count: Union[str, int, float]) -> Optional[int]:
        """Clean and validate statistical count fields (watches, likes, lists, etc.)."""
        if pd.isna(count) or count == '':
            return None
        
        if isinstance(count, (int, float)):
            return int(count) if count >= 0 else None
        
        if isinstance(count, str):
            # Handle cases like "712K", "1.5M", "183K"
            try:
                return self._parse_stat_number_string(count)
            except (ValueError, TypeError):
                # Try to extract just numbers
                numbers = re.findall(r'\d+', count)
                if numbers:
                    return int(numbers[0])
        
        return None
    
    def _parse_stat_number_string(self, text: str) -> Optional[int]:
        """Parse a statistic number from text like '712K', '1.5M'."""
        if not text:
            return None
        
        import re
        # Match patterns like "712K", "1.5M", "183,456"
        match = re.search(r'([\d,]+(?:\.\d+)?)\s*([KMB]?)', text.replace(',', ''))
        if match:
            num = float(match.group(1))
            multiplier = match.group(2).upper()
            
            if multiplier == 'K':
                num *= 1000
            elif multiplier == 'M':
                num *= 1000000
            elif multiplier == 'B':
                num *= 1000000000
            
            return int(num)
        
        return None
    
    def _clean_ratings_breakdown(self, breakdown: Union[str, dict]) -> Optional[str]:
        """Clean and validate ratings breakdown data, returning JSON string for DataFrame compatibility."""
        if pd.isna(breakdown) or breakdown == '':
            return None
        
        if isinstance(breakdown, str):
            # If already a string, try to validate it's valid JSON
            try:
                import json
                json.loads(breakdown)  # Validate it's valid JSON
                return breakdown
            except (json.JSONDecodeError, ValueError):
                return None
        
        if not isinstance(breakdown, dict):
            return None
        
        # Validate breakdown structure
        cleaned_breakdown = {}
        for star_rating, data in breakdown.items():
            if isinstance(data, dict) and 'count' in data and 'percentage' in data:
                try:
                    cleaned_breakdown[star_rating] = {
                        'count': int(data['count']),
                        'percentage': float(data['percentage'])
                    }
                except (ValueError, TypeError):
                    continue
        
        if cleaned_breakdown:
            # Convert to JSON string for DataFrame compatibility
            import json
            return json.dumps(cleaned_breakdown)
        
        return None
    
    def _standardize_genre(self, genre: str) -> str:
        """Standardize genre names."""
        genre_mapping = {
            'sci-fi': 'Science Fiction',
            'scifi': 'Science Fiction',
            'science-fiction': 'Science Fiction',
            'dramedy': 'Comedy-Drama',
            'rom-com': 'Romantic Comedy',
            'action/adventure': 'Action',
            'documentary': 'Documentary',
            'doc': 'Documentary'
        }
        
        genre_lower = genre.lower()
        return genre_mapping.get(genre_lower, genre.title())
    
    def _standardize_country(self, country: str) -> str:
        """Standardize country names."""
        country_mapping = {
            'usa': 'United States',
            'us': 'United States',
            'united states of america': 'United States',
            'uk': 'United Kingdom',
            'britain': 'United Kingdom',
            'great britain': 'United Kingdom',
            'south korea': 'South Korea',
            'korea': 'South Korea'
        }
        
        country_lower = country.lower()
        return country_mapping.get(country_lower, country)


class FilmDataFrameBuilder:
    """
    Builder class for creating specialized DataFrames from Letterboxd film data.
    """
    
    def __init__(self, parser: LetterboxdParser = None):
        self.parser = parser or LetterboxdParser()
    
    def build_films_dataframe(self, films_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Build a comprehensive films DataFrame.
        
        Args:
            films_data: List of film dictionaries
            
        Returns:
            DataFrame with film information
        """
        df = self.parser.parse_to_dataframe(films_data)
        
        if df.empty:
            return df
        
        # Add computed columns
        df = self._add_computed_columns(df)
        
        # Reorder columns for better readability
        df = self._reorder_columns(df)
        
        return df
    
    def build_list_summary_dataframe(self, films_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Build a summary DataFrame for list analysis.
        
        Args:
            films_data: List of film dictionaries
            
        Returns:
            Summary DataFrame
        """
        df = self.build_films_dataframe(films_data)
        
        if df.empty:
            return df
        
        # Create summary statistics
        summary_data = []
        
        # Overall statistics
        summary_data.append({
            'metric': 'Total Films',
            'value': len(df),
            'description': 'Total number of films in the list'
        })
        
        # Year statistics
        if 'year' in df.columns:
            years = df['year'].dropna()
            if not years.empty:
                summary_data.extend([
                    {
                        'metric': 'Earliest Film',
                        'value': int(years.min()),
                        'description': 'Year of the earliest film'
                    },
                    {
                        'metric': 'Latest Film',
                        'value': int(years.max()),
                        'description': 'Year of the most recent film'
                    },
                    {
                        'metric': 'Average Year',
                        'value': round(years.mean(), 1),
                        'description': 'Average release year'
                    }
                ])
        
        # Rating statistics
        if 'owner_rating' in df.columns:
            ratings = df['owner_rating'].dropna()
            if not ratings.empty:
                summary_data.extend([
                    {
                        'metric': 'Average Rating',
                        'value': round(ratings.mean(), 2),
                        'description': 'Average user rating'
                    },
                    {
                        'metric': 'Highest Rating',
                        'value': ratings.max(),
                        'description': 'Highest rated film'
                    },
                    {
                        'metric': 'Rated Films',
                        'value': len(ratings),
                        'description': 'Number of films with ratings'
                    }
                ])
        
        # Country diversity
        if 'countries' in df.columns:
            all_countries = []
            for countries in df['countries'].dropna():
                if isinstance(countries, list):
                    all_countries.extend(countries)
            
            unique_countries = len(set(all_countries))
            summary_data.append({
                'metric': 'Countries Represented',
                'value': unique_countries,
                'description': 'Number of different countries'
            })
        
        return pd.DataFrame(summary_data)
    
    def build_genre_analysis_dataframe(self, films_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Build a DataFrame for genre analysis.
        
        Args:
            films_data: List of film dictionaries
            
        Returns:
            Genre analysis DataFrame
        """
        df = self.build_films_dataframe(films_data)
        
        if df.empty or 'genres' not in df.columns:
            return pd.DataFrame()
        
        # Extract all genres
        genre_counts = {}
        for genres in df['genres'].dropna():
            if isinstance(genres, list):
                for genre in genres:
                    genre_counts[genre] = genre_counts.get(genre, 0) + 1
        
        # Create genre DataFrame
        genre_df = pd.DataFrame([
            {'genre': genre, 'count': count, 'percentage': (count / len(df)) * 100}
            for genre, count in sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)
        ])
        
        return genre_df
    
    def _add_computed_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add computed columns to the DataFrame."""
        # Add decade column
        if 'year' in df.columns:
            df['decade'] = (df['year'] // 10) * 10
        
        # Add has_rating flag
        if 'owner_rating' in df.columns:
            df['has_rating'] = df['owner_rating'].notna()
        
        # Add genre count
        if 'genres' in df.columns:
            df['genre_count'] = df['genres'].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
        
        # Add cast count
        if 'cast' in df.columns:
            df['cast_count'] = df['cast'].apply(
                lambda x: len(x) if isinstance(x, list) else 0
            )
        
        return df
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns for better readability."""
        # Define preferred column order
        preferred_order = [
            'list_position', 'name', 'title', 'year', 'decade',
            'director', 'owner_rating', 'has_rating',
            'countries', 'genres', 'genre_count',
            'cast', 'cast_count', 'runtime',
            'film_id', 'film_slug', 'target_link',
            'original_title', 'tagline', 'synopsis'
        ]
        
        # Reorder existing columns
        ordered_columns = []
        for col in preferred_order:
            if col in df.columns:
                ordered_columns.append(col)
        
        # Add any remaining columns
        remaining_columns = [col for col in df.columns if col not in ordered_columns]
        ordered_columns.extend(remaining_columns)
        
        return df[ordered_columns]


def create_letterboxd_dataframe(films_data: List[Dict[str, Any]], 
                               clean_data: bool = True) -> pd.DataFrame:
    """
    Convenience function to create a Letterboxd DataFrame.
    
    Args:
        films_data: List of film dictionaries
        clean_data: Whether to apply data cleaning
        
    Returns:
        Cleaned and formatted DataFrame
    """
    parser = LetterboxdParser(clean_data=clean_data)
    builder = FilmDataFrameBuilder(parser)
    return builder.build_films_dataframe(films_data)


def create_summary_dataframe(films_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convenience function to create a summary DataFrame.
    
    Args:
        films_data: List of film dictionaries
        
    Returns:
        Summary statistics DataFrame
    """
    builder = FilmDataFrameBuilder()
    return builder.build_list_summary_dataframe(films_data)


def create_genre_analysis_dataframe(films_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convenience function to create a genre analysis DataFrame.
    
    Args:
        films_data: List of film dictionaries
        
    Returns:
        Genre analysis DataFrame
    """
    builder = FilmDataFrameBuilder()
    return builder.build_genre_analysis_dataframe(films_data)
