#!/usr/bin/env python3
"""
List Creation Module for Letterboxd Data
Provides various methods to create interesting lists from film JSON data.
"""

import json
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from pathlib import Path
import pandas as pd
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class SortBy(Enum):
    """Enumeration of available sorting options."""
    AVERAGE_RATING = "average_rating"
    RELEASE_YEAR = "release_year"
    RUNTIME = "runtime"
    ALPHABETICAL = "name"
    LISTPOSITION = "list_position"
    WATCHES = "watches_count_exact"

@dataclass
class ListConfig:
    """Configuration for list creation."""
    title: str
    description: str
    limit: Optional[int] = None
    sort_by: SortBy = SortBy.AVERAGE_RATING
    sort_ascending: bool = False
    countries: Optional[List[str]] = None
    languages: Optional[List[str]] = None
    include_secondary_languages: bool = False
    genres: Optional[List[str]] = None
    min_year: Optional[int] = None
    max_year: Optional[int] = None
    min_runtime: Optional[int] = None
    max_runtime: Optional[int] = None
    min_rating: Optional[float] = None
    max_rating: Optional[float] = None
    cutoff_type: Optional[str] = None
    cutoff_limit: Optional[int] = None

class ListCreator:
    """
    Main class for creating various types of lists from film JSON data.
    """
    
    def __init__(self, json_files: List[str]):
        """
        Initialize the ListCreator with JSON film data files.
        
        Args:
            json_files: A list of paths to JSON files containing film data. This 
                        allows for combining data from multiple sources. Files can
                        be lists of films or dictionaries mapping film_id to stats.
        """
        self.json_files = json_files
        self.films_data = []
        self.stats_data = {}
        self._load_and_merge_data()

    def _load_and_merge_data(self) -> None:
        """
        Load data from all provided JSON files and merge them.
        - All files containing lists are combined
        - Films with the same film_id are merged, with later files taking precedence for conflicting fields
        """
        all_films = []
        films_by_id = {}

        for file_path in self.json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_films.extend(data)
                        logger.info(f"Loaded {len(data)} films from file: {file_path}")
                    elif isinstance(data, dict):
                        # Convert dict to list of items for consistent processing
                        for key, value in data.items():
                            if isinstance(value, dict):
                                value['film_id'] = key
                                all_films.append(value)
                        logger.info(f"Loaded {len(data)} records from dict file: {file_path}")
                    else:
                        logger.warning(f"File {file_path} contains data of an unsupported type.")
            except Exception as e:
                logger.error(f"Error loading or parsing {file_path}: {e}")

        # Merge films by film_id, with later entries taking precedence
        for film in all_films:
            film_id = str(film.get('film_id', ''))
            if film_id:
                if film_id in films_by_id:
                    # Merge the films, with the new film taking precedence
                    films_by_id[film_id].update(film)
                else:
                    films_by_id[film_id] = film.copy()

        # Convert back to list
        self.films_data = list(films_by_id.values())
        
        logger.info(f"Total unique films loaded: {len(self.films_data)}")

    def _load_stats_data(self, stats_file: str) -> None:
        """Load film statistics from a JSON file."""
        try:
            with open(stats_file, 'r', encoding='utf-8') as f:
                self.stats_data = json.load(f)
        except Exception as e:
            logger.error(f"Error loading stats file {stats_file}: {e}")
    
    def _load_all_films(self) -> None:
        """Load film data from all JSON files."""
        for file_path in self.json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.films_data.extend(data)
                    else:
                        logger.warning(f"File {file_path} does not contain a list of films")
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
    
    def _normalize_film_data(self, film: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize film data to ensure consistent field names and types.
        
        Args:
            film: Raw film data dictionary
            
        Returns:
            Normalized film data dictionary
        """
        normalized = {}
        
        # Handle different naming conventions from your JSON files
        normalized['name'] = film.get('title') or film.get('name') or film.get('Name', 'Unknown')
        normalized['film_id'] = film.get('film_id') or film.get('Date', 'unknown')
        
        # Extract year from Tags field or release_year
        year = film.get('release_year') or film.get('year') or film.get('Tags', 0)
        if isinstance(year, str):
            try:
                normalized['release_year'] = int(year) if year else 0
            except (ValueError, TypeError):
                normalized['release_year'] = 0
        else:
            normalized['release_year'] = year if year else 0
        
        # Director from Description field
        normalized['director'] = film.get('director') or film.get('Description', '')
        
        # URL for film page - try multiple field names
        url = (film.get('url') or 
               film.get('URL') or 
               film.get('target_link') or 
               film.get('film_link') or '')
        
        # If we have a film_slug but no full URL, construct it
        if not url and film.get('film_slug'):
            url = f"/film/{film.get('film_slug')}/"
        
        # Convert relative URLs to full Letterboxd URLs
        if url and url.startswith('/'):
            normalized['url'] = f"https://letterboxd.com{url}"
        elif url and not url.startswith('http'):
            # Handle cases where URL might not have leading slash
            normalized['url'] = f"https://letterboxd.com/{url.lstrip('/')}"
        else:
            normalized['url'] = url or ''
        
        # Fields that may or may not exist in your data
        normalized['countries'] = film.get('countries', [])
        normalized['primary_language'] = film.get('primary_language', '')
        normalized['other_languages'] = film.get('other_languages', [])
        normalized['genres'] = film.get('genres', [])
        normalized['runtime'] = film.get('runtime', 0)
        
        # Rating fields - these may come from the merged data
        normalized['average_rating'] = film.get('average_rating', 0.0)
        normalized['ratings_count'] = film.get('total_ratings', film.get('ratings_count', 0))
        normalized['watches_count'] = film.get('watches_count', 0)
        normalized['watches_count_exact'] = film.get('watches_count_exact', 0)
        
        # Convert string numbers to appropriate types
        try:
            if isinstance(normalized['average_rating'], str):
                normalized['average_rating'] = float(normalized['average_rating']) if normalized['average_rating'] else 0.0
            if isinstance(normalized['runtime'], str):
                normalized['runtime'] = int(normalized['runtime']) if normalized['runtime'] else 0
        except (ValueError, TypeError):
            pass  # Keep original values if conversion fails
        
        return normalized
    
    def _filter_films(self, 
                     films: List[Dict[str, Any]], 
                     countries: Optional[List[str]] = None,
                     languages: Optional[List[str]] = None,
                     include_secondary_languages: bool = False,
                     genres: Optional[List[str]] = None,
                     min_year: Optional[int] = None,
                     max_year: Optional[int] = None,
                     min_runtime: Optional[int] = None,
                     max_runtime: Optional[int] = None,
                     min_rating: Optional[float] = None,
                     max_rating: Optional[float] = None,
                     cutoff_type: Optional[str] = None,
                     cutoff_limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Filter films based on various criteria.
        
        Args:
            films: List of film dictionaries
            countries: List of countries to filter by
            languages: List of languages to filter by
            include_secondary_languages: Include secondary languages in the language filter
            genres: List of genres to filter by
            min_year: Minimum release year
            max_year: Maximum release year
            min_runtime: Minimum runtime in minutes
            max_runtime: Maximum runtime in minutes
            min_rating: Minimum average rating
            max_rating: Maximum average rating
            
        Returns:
            Filtered list of films
        """
        filtered = films
        
        if countries:
            filtered = [f for f in filtered 
                       if any(country.lower() in [c.lower() for c in f.get('countries', [])] 
                              for country in countries)]
        
        if languages:
            language_set = {lang.lower() for lang in languages}
            
            def has_language(film: Dict[str, Any]) -> bool:
                primary_language = film.get('primary_language', '').lower()
                if primary_language in language_set:
                    return True
                
                if include_secondary_languages:
                    other_languages = film.get('other_languages', [])
                    if any(lang.lower() in language_set for lang in other_languages):
                        return True
                
                return False
            
            filtered = [f for f in filtered if has_language(f)]
        
        if genres:
            filtered = [f for f in filtered 
                       if any(genre.lower() in [g.lower() for g in f.get('genres', [])] 
                             for genre in genres)]
        
        if min_year:
            filtered = [f for f in filtered if f.get('release_year', 0) >= min_year]
        
        if max_year:
            filtered = [f for f in filtered if f.get('release_year', 0) <= max_year]
        
        if min_runtime:
            filtered = [f for f in filtered if f.get('runtime', 0) >= min_runtime]
        
        if max_runtime:
            filtered = [f for f in filtered if f.get('runtime', 0) <= max_runtime]

        if min_rating:
            filtered = [f for f in filtered if f.get('average_rating', 0.0) >= min_rating]

        if max_rating:
            filtered = [f for f in filtered if f.get('average_rating', 0.0) <= max_rating]
        
        # Apply cutoff filtering
        if cutoff_type and cutoff_limit:
            if cutoff_type == 'ratings':
                # Filter by minimum number of ratings
                filtered = [f for f in filtered if f.get('ratings_count', 0) >= cutoff_limit]
                logger.info(f"Applied ratings cutoff: {cutoff_limit} minimum ratings")
            elif cutoff_type == 'watches':
                # Filter by minimum number of watches - check both exact and approximate counts
                filtered = [f for f in filtered if 
                          max(f.get('watches_count_exact', 0), f.get('watches_count', 0)) >= cutoff_limit]
                logger.info(f"Applied watches cutoff: {cutoff_limit} minimum watches")
        
        return filtered
    
    def _sort_films(self, 
                   films: List[Dict[str, Any]], 
                   sort_by: SortBy = SortBy.AVERAGE_RATING,
                   ascending: bool = False) -> List[Dict[str, Any]]:
        """
        Sort films by specified criteria.
        
        Args:
            films: List of film dictionaries
            sort_by: Field to sort by
            ascending: Sort in ascending order if True, descending if False
            
        Returns:
            Sorted list of films
        """
        sort_key = sort_by.value
        
        # Handle special sorting cases
        if sort_by == SortBy.ALPHABETICAL:
            return sorted(films, key=lambda x: x.get('name', '').lower(), reverse=not ascending)
        elif sort_by == SortBy.LISTPOSITION:
            return sorted(films, key=lambda x: x.get('list_position', 999999), reverse=not ascending)
        else:
            return sorted(films, 
                         key=lambda x: x.get(sort_key, 0) if x.get(sort_key, 0) is not None else 0, 
                         reverse=not ascending)
    
    def create_list(
        self,
        config: ListConfig,
        output_path: Path,
        output_format: str = 'json',
        cutoff: Optional[Tuple[str, Union[int, float]]] = None,
        simple_json: bool = False
    ) -> Dict[str, Any]:
        """
        Create a list of films based on the provided configuration.
        
        Args:
            config: ListConfig object with title, description, limit, and filter criteria
            output_path: Path to save the output file
            output_format: Format of the output file ('json' or 'csv')
            cutoff: Optional cutoff criteria for filtering films (deprecated, use config)
            simple_json: Flag to produce simplified JSON output
            
        Returns:
            Dictionary with list information including films and metadata
        """
        # Step 1: Normalize all film data
        normalized_films = [self._normalize_film_data(film) for film in self.films_data]
        
        # Post-merge validation: Check if sort_by key exists
        sort_key = config.sort_by.value
        if not any(sort_key in film for film in normalized_films):
            raise ValueError(
                f"The sort key '{sort_key}' is not present in the merged data. "
                "Please ensure at least one of the JSON files contains this field."
            )

        # Debugging: Log a sample of normalized films
        if normalized_films:
            logger.info(f"Sample of normalized films: {json.dumps(normalized_films[:2], indent=2)}")

        # Step 2: Filter films based on criteria from config
        filtered_films = self._filter_films(
            normalized_films,
            countries=config.countries,
            languages=config.languages,
            include_secondary_languages=config.include_secondary_languages,
            genres=config.genres,
            min_year=config.min_year,
            max_year=config.max_year,
            min_runtime=config.min_runtime,
            max_runtime=config.max_runtime,
            min_rating=config.min_rating,
            max_rating=config.max_rating,
            cutoff_type=config.cutoff_type,
            cutoff_limit=config.cutoff_limit
        )
        
        # Debugging: Log filter counts
        logger.info(f"Initial film count: {len(normalized_films)}")
        logger.info(f"Film count after filtering: {len(filtered_films)}")

        # Step 3: Sort the filtered films
        sorted_films = self._sort_films(
            filtered_films, 
            sort_by=config.sort_by, 
            ascending=config.sort_ascending
        )
        
        # Debugging: Log sort counts
        logger.info(f"Film count after sorting: {len(sorted_films)}")

        # Step 4: Limit the number of films
        if config.limit is not None:
            limited_films = sorted_films[:config.limit]
        else:
            limited_films = sorted_films

        # Debugging: Log limit counts
        logger.info(f"Film count after limiting: {len(limited_films)}")

        # Step 5: Generate output
        self._generate_output(limited_films, config, output_path, output_format, simple_json=simple_json)

        logger.info(f"Successfully created list: {output_path}")
        
        # Prepare filters information for the return data
        filters_info = {}
        if config.cutoff_type and config.cutoff_limit:
            filters_info['cutoff_type'] = config.cutoff_type
            filters_info['cutoff_limit'] = config.cutoff_limit
        
        # Return the data for use by calling code
        result = {
            'title': config.title,
            'description': config.description,
            'total_found': len(filtered_films),
            'films_returned': len(limited_films),
            'films': limited_films
        }
        
        # Only add filters if there are any
        if filters_info:
            result['filters'] = filters_info
            
        return result

    def _generate_output(self, films: List[Dict[str, Any]], config: ListConfig, output_path: Path, output_format: str, simple_json: bool = False) -> None:
        """Generate the output file in the specified format."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_format == 'json':
            if simple_json:
                output_data = [{"film_id": int(film.get("film_id", 0)), "name": film.get("name")} for film in films]
            else:
                output_data = {
                    "title": config.title,
                    "description": config.description,
                    "film_count": len(films),
                    "films": films
                }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4)

        elif output_format == 'csv':
            df = pd.DataFrame(films)
            df.to_csv(output_path, index=False)
    
    def get_available_countries(self) -> List[str]:
        """Get a list of all available countries in the dataset."""
        countries = set()
        for film in self.films_data:
            normalized = self._normalize_film_data(film)
            countries.update(normalized.get('countries', []))
        return sorted(list(countries))
    
    def get_available_languages(self) -> List[str]:
        """Get a list of all available languages in the dataset."""
        languages = set()
        for film in self.films_data:
            normalized = self._normalize_film_data(film)
            if normalized.get('primary_language'):
                languages.add(normalized['primary_language'])
            languages.update(normalized.get('other_languages', []))
        return sorted(list(languages))
    
    def get_available_genres(self) -> List[str]:
        """Get a list of all available genres in the dataset."""
        genres = set()
        for film in self.films_data:
            normalized = self._normalize_film_data(film)
            genres.update(normalized.get('genres', []))
        return sorted(list(genres))
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get basic statistics about the loaded dataset."""
        normalized_films = [self._normalize_film_data(film) for film in self.films_data]
        
        # Extract years and ratings
        years = [f['release_year'] for f in normalized_films if f['release_year'] > 0]
        ratings = [f['average_rating'] for f in normalized_films if f['average_rating'] > 0]
        runtimes = [f['runtime'] for f in normalized_films if f['runtime'] > 0]
        
        return {
            'total_films': len(self.films_data),
            'films_with_ratings': len(ratings),
            'films_with_years': len(years),
            'films_with_runtime': len(runtimes),
            'year_range': {
                'min': min(years) if years else None,
                'max': max(years) if years else None
            },
            'rating_range': {
                'min': min(ratings) if ratings else None,
                'max': max(ratings) if ratings else None,
                'average': sum(ratings) / len(ratings) if ratings else None
            },
            'runtime_range': {
                'min': min(runtimes) if runtimes else None,
                'max': max(runtimes) if runtimes else None,
                'average': sum(runtimes) / len(runtimes) if runtimes else None
            },
            'unique_countries': len(self.get_available_countries()),
            'unique_languages': len(self.get_available_languages()),
            'unique_genres': len(self.get_available_genres())
        }

    def create_country_language_list(
        self,
        limit: int = 100,
        countries: Optional[List[str]] = None,
        languages: Optional[List[str]] = None,
        sort_by: SortBy = SortBy.AVERAGE_RATING
    ) -> Dict[str, Any]:
        """
        Create a list filtered by countries and/or languages.
        
        Args:
            limit: Maximum number of films to return
            countries: List of countries to filter by
            languages: List of languages to filter by
            sort_by: Field to sort by
            
        Returns:
            Dictionary with list information
        """
        # Build title based on filters
        title_parts = []
        if countries:
            title_parts.append(f"{', '.join(countries)} Films")
        if languages:
            title_parts.append(f"{', '.join(languages)} Language Films")
        
        if not title_parts:
            title = f"Top {limit} Films"
        else:
            title = f"Top {limit} {' & '.join(title_parts)}"
        
        # Create config
        config = ListConfig(
            title=title,
            description=f"Top {limit} films filtered by specified criteria",
            limit=limit,
            sort_by=sort_by,
            sort_ascending=False,
            countries=countries,
            languages=languages
        )
        
        # Process films
        normalized_films = [self._normalize_film_data(film) for film in self.films_data]
        
        # Filter films
        filtered_films = self._filter_films(
            normalized_films,
            countries=countries,
            languages=languages
        )
        
        # Sort films
        sorted_films = self._sort_films(filtered_films, sort_by=sort_by, ascending=False)
        
        # Limit results
        limited_films = sorted_films[:limit] if limit else sorted_films
        
        return {
            'title': title,
            'description': config.description,
            'total_found': len(filtered_films),
            'films_returned': len(limited_films),
            'films': limited_films
        }
