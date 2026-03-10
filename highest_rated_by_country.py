#!/usr/bin/env python3
"""
Script to find the highest rated film from each represented country.
Can be easily adapted for other grouping criteria (e.g., by decade, genre, etc.)

Usage:
    python highest_rated_by_country.py input_file1 [input_file2 ...] [--output output_file]

Examples:
    python highest_rated_by_country.py scrape_scripts/output/all_the_films_detailed_20250821_064903.json scrape_scripts/output/all_the_films_ratings_stats_20250820_035741.json
    python highest_rated_by_country.py scrape_scripts/output/all_the_films_detailed_20250821_064903.json scrape_scripts/output/all_the_films_ratings_stats_20250820_035741.json --output highest_by_country.csv
"""

import json
import argparse
import csv
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional

# Import the country language mapping
from country_languages import COUNTRY_LANGUAGES
print("✅ Using country language filtering (50+ films with percentage-based thresholds)")


def load_film_data(input_files: List[str]) -> List[Dict[str, Any]]:
    """Load and merge film data from multiple JSON files."""
    print(f"📁 Loading film data from {len(input_files)} file(s)...")
    
    merged_films = {}  # Use dict to merge by film_id/film_slug
    
    for input_file in input_files:
        print(f"   Loading {input_file}...")
        
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"   ✅ Loaded {len(data)} films from {Path(input_file).name}")
        
        # Merge films by film_id or film_slug
        for film in data:
            # Determine unique identifier
            film_key = film.get('film_id') or film.get('film_slug') or film.get('name', 'unknown')
            
            if film_key in merged_films:
                # Merge data, with new data taking precedence
                merged_films[film_key].update(film)
            else:
                merged_films[film_key] = film.copy()
    
    merged_list = list(merged_films.values())
    print(f"✅ Total merged films: {len(merged_list)}")
    return merged_list


def normalize_film_data(film: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize film data to ensure consistent field access."""
    normalized = {}
    
    # Basic film info
    normalized['name'] = film.get('title') or film.get('name') or film.get('Name') or 'Unknown'
    normalized['year'] = film.get('year') or film.get('release_year') or film.get('Tags') or 0
    normalized['director'] = film.get('director') or film.get('Description') or 'Unknown'
    normalized['countries'] = film.get('countries', [])
    normalized['primary_language'] = film.get('primary_language', '')
    normalized['runtime'] = film.get('runtime', 0)
    
    # Rating info - try multiple possible field names
    normalized['average_rating'] = (
        film.get('average_rating') or 
        film.get('rating') or 
        0.0
    )
    
    # Convert to float if it's a string
    try:
        normalized['average_rating'] = float(normalized['average_rating'])
    except (ValueError, TypeError):
        normalized['average_rating'] = 0.0
    
    # Convert year to int if it's a string
    try:
        if isinstance(normalized['year'], str) and normalized['year'].isdigit():
            normalized['year'] = int(normalized['year'])
        elif not isinstance(normalized['year'], int):
            normalized['year'] = 0
    except (ValueError, TypeError):
        normalized['year'] = 0
    
    # Additional metadata
    normalized['film_id'] = film.get('film_id') or film.get('Date') or film.get('id') or ''
    normalized['film_slug'] = film.get('film_slug', '')
    normalized['genres'] = film.get('genres', [])
    normalized['themes'] = film.get('themes', [])
    
    # Watch and rating statistics
    normalized['total_ratings'] = film.get('total_ratings', 0)
    normalized['watches_count'] = film.get('watches_count', 0)
    normalized['fans_count'] = film.get('fans_count', 0)
    
    # URL construction
    url = film.get('url') or film.get('URL')
    if url:
        if not url.startswith('http'):
            normalized['url'] = f"https://letterboxd.com{url}"
        else:
            normalized['url'] = url
    elif film.get('target_link'):
        normalized['url'] = f"https://letterboxd.com{film['target_link']}"
    elif film.get('film_slug'):
        normalized['url'] = f"https://letterboxd.com/film/{film['film_slug']}/"
    else:
        normalized['url'] = ''
    
    return normalized


def get_country_languages():
    """Get the data-driven country language mapping."""
    return COUNTRY_LANGUAGES

def calculate_film_score(film: Dict[str, Any], country: str, country_languages: Dict[str, List[str]], used_films: set) -> float:
    """
    Calculate a score for how well a film represents a country.
    Higher score = better representation.
    """
    if film['name'] in used_films:
        return -1  # Film already used for another country
    
    score = film['average_rating']  # Base score is the rating
    
    # Smart language bonus system
    if country in country_languages:
        expected_languages = country_languages[country]
        primary_lang = film['primary_language']
        
        if not expected_languages:
            # Empty list means "accept any language" (like Switzerland with many languages)
            score += 0.5  # Small bonus for diverse countries
        else:
            # Check for exact language matches (case-insensitive)
            expected_lower = [lang.lower() for lang in expected_languages]
            if primary_lang.lower() in expected_lower:
                score += 3.0  # Strong bonus for native language match
            else:
                score -= 1.0  # Penalty for non-native language in focused countries
    
    # Country priority bonus: higher if this country appears first in the countries list
    countries = film['countries']
    if countries and countries[0] == country:
        score += 1.5  # Main production country bonus
    elif country in countries[:2]:
        score += 0.7  # Secondary production country bonus
    elif len(countries) > 1:
        score -= 0.3  # Small penalty for minor co-production role
    
    # Penalize heavy co-productions (dilutes national character)
    if len(countries) > 4:
        score -= 1.0
    elif len(countries) > 2:
        score -= 0.3
    
    return score

def find_highest_rated_by_country(films: List[Dict[str, Any]], min_rating: float = 0.0) -> Dict[str, Dict[str, Any]]:
    """
    Find the highest rated film from each country, avoiding duplicates and preferring language matches.
    
    Args:
        films: List of film dictionaries
        min_rating: Minimum rating threshold (films below this are ignored)
    
    Returns:
        Dictionary mapping country name to the highest rated film from that country
    """
    print(f"🔍 Finding highest rated film from each country...")
    print(f"🎯 Minimum rating threshold: {min_rating}")
    print(f"🌐 Prioritizing language matches and avoiding duplicates...")
    
    country_languages = get_country_languages()
    country_candidates = defaultdict(list)
    country_film_counts = defaultdict(int)
    used_films = set()
    
    # First pass: collect all candidate films for each country
    for film in films:
        normalized = normalize_film_data(film)
        countries = normalized['countries']
        rating = normalized['average_rating']
        
        # Skip films without rating data or below threshold
        if not rating or rating < min_rating:
            continue
            
        # Add film as candidate for each of its countries
        for country in countries:
            if not country:  # Skip empty country names
                continue
                
            country_film_counts[country] += 1
            country_candidates[country].append(normalized)
    
    # Second pass: select best film for each country, prioritizing unused films and language matches
    country_best_films = {}
    
    # Sort countries by number of films (fewer films = higher priority to pick first)
    # This ensures countries with fewer options get their best films before competition
    countries_by_film_count = sorted(country_film_counts.items(), key=lambda x: x[1])
    
    for country, film_count in countries_by_film_count:
        candidates = country_candidates[country]
        
        # Calculate scores for all candidates
        scored_candidates = []
        for film in candidates:
            score = calculate_film_score(film, country, country_languages, used_films)
            if score >= 0:  # Only consider films that haven't been used
                scored_candidates.append((score, film))
        
        if scored_candidates:
            # Sort by score (highest first) and pick the best
            scored_candidates.sort(key=lambda x: x[0], reverse=True)
            best_score, best_film = scored_candidates[0]
            
            country_best_films[country] = best_film
            country_best_films[country]['country_film_count'] = film_count
            country_best_films[country]['selection_score'] = best_score
            
            # Mark this film as used
            used_films.add(best_film['name'])
    
    print(f"✅ Found unique films from {len(country_best_films)} countries")
    print(f"📊 Countries with most films:")
    
    # Show top countries by film count
    top_countries = sorted(country_film_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    for country, count in top_countries:
        print(f"   {country}: {count} films")
    
    return country_best_films


def save_results(results: Dict[str, Dict[str, Any]], output_file: str, format_type: str = 'csv'):
    """Save results to file."""
    output_path = Path(output_file)
    
    if format_type == 'csv':
        print(f"💾 Saving results to {output_file} (CSV format)")
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'country', 'name', 'year', 'director', 'average_rating', 
                'total_ratings', 'watches_count', 'fans_count',
                'runtime', 'primary_language', 'genres', 'themes',
                'country_film_count', 'url'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Sort alphabetically by country name
            sorted_results = sorted(results.items(), key=lambda x: x[0])
            
            for country, film in sorted_results:
                row = {
                    'country': country,
                    'name': film['name'],
                    'year': film['year'],
                    'director': film['director'],
                    'average_rating': film['average_rating'],
                    'total_ratings': film.get('total_ratings', 0),
                    'watches_count': film.get('watches_count', 0),
                    'fans_count': film.get('fans_count', 0),
                    'runtime': film['runtime'],
                    'primary_language': film['primary_language'],
                    'genres': ', '.join(film['genres'][:3]),  # Limit to top 3 genres
                    'themes': ', '.join(film['themes'][:2]) if film.get('themes') else '',  # Limit to top 2 themes
                    'country_film_count': film.get('country_film_count', 0),
                    'url': film['url']
                }
                writer.writerow(row)
    
    elif format_type == 'json':
        print(f"💾 Saving results to {output_file} (JSON format)")
        
        # Convert to a more structured format for JSON
        json_output = {
            'title': 'Highest Rated Film from Each Country',
            'description': 'The highest rated film from each represented country',
            'total_countries': len(results),
            'films': []
        }
        
        # Sort alphabetically by country name
        sorted_results = sorted(results.items(), key=lambda x: x[0])
        
        for country, film in sorted_results:
            film_entry = film.copy()
            film_entry['country'] = country
            json_output['films'].append(film_entry)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_output, f, indent=2, ensure_ascii=False)


def display_summary(results: Dict[str, Dict[str, Any]]):
    """Display a summary of the results."""
    print(f"\n🏆 HIGHEST RATED FILMS BY COUNTRY")
    print("=" * 80)
    
    # Sort alphabetically by country name
    sorted_results = sorted(results.items(), key=lambda x: x[0])
    
    print(f"🌍 Found films from {len(results)} countries")
    print(f"📋 All countries (sorted alphabetically):")
    print()
    
    for i, (country, film) in enumerate(sorted_results, 1):
        rating = film['average_rating']
        name = film['name']
        year = film['year']
        director = film['director']
        
        print(f"{i:3}. {country:<25} ⭐ {rating:.2f} - {name} ({year}) - {director}")
    
    print()
    print(f"💡 Use --output to save full results to CSV or JSON")


def main():
    parser = argparse.ArgumentParser(
        description='Find the highest rated film from each represented country',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python highest_rated_by_country.py scrape_scripts/output/all_the_films_detailed_20250821_064903.json scrape_scripts/output/all_the_films_ratings_stats_20250820_035741.json
  python highest_rated_by_country.py detailed.json ratings.json --output results.csv --min-rating 3.5
  python highest_rated_by_country.py detailed.json ratings.json --output results.json --format json

Adaptations:
  To group by decade instead of country, modify the grouping logic in find_highest_rated_by_country()
  To find by genre, change the grouping key from 'countries' to 'genres'
  To find lowest rated instead, change the comparison operator in the main loop
        """
    )
    
    parser.add_argument('input_files', nargs='+',
                       help='Path(s) to JSON file(s) containing film data (will be merged)')
    parser.add_argument('--output', '-o', 
                       help='Output file path (auto-detects format from extension)')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv',
                       help='Output format (default: csv)')
    parser.add_argument('--min-rating', type=float, default=0.0,
                       help='Minimum rating threshold (default: 0.0)')
    
    args = parser.parse_args()
    
    # Load the data
    try:
        films = load_film_data(args.input_files)
    except FileNotFoundError as e:
        print(f"❌ Error: File not found: {e}")
        return 1
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON: {e}")
        return 1
    
    # Find highest rated films by country
    results = find_highest_rated_by_country(films, args.min_rating)
    
    if not results:
        print("❌ No films found matching criteria")
        return 1
    
    # Display summary
    display_summary(results)
    
    # Save results if requested
    if args.output:
        # Auto-detect format from extension if not specified
        format_type = args.format
        if args.output.endswith('.json'):
            format_type = 'json'
        elif args.output.endswith('.csv'):
            format_type = 'csv'
        
        save_results(results, args.output, format_type)
        print(f"✅ Full results saved to {args.output}")
    
    return 0


if __name__ == "__main__":
    exit(main())
