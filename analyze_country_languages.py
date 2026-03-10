#!/usr/bin/env python3
"""
Analyze the actual languages used in films for each country to create
a proper country-to-languages mapping for the highest_rated_by_country script.
"""

import json
import sys
from collections import defaultdict, Counter
from typing import Dict, List, Any, Optional

def load_film_data(file_path: str) -> List[Dict[str, Any]]:
    """Load film data from JSON file."""
    print(f"📁 Loading film data from {file_path}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"✅ Loaded {len(data)} films")
    return data

def analyze_languages_by_country(films: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    """
    Analyze what languages are actually used in films for each country.
    
    Returns:
        Dictionary mapping country -> {language: count}
    """
    country_languages = defaultdict(lambda: defaultdict(int))
    
    for film in films:
        countries = film.get('countries', [])
        if not isinstance(countries, list):
            continue
            
        primary_lang = film.get('primary_language', '')
        other_langs = film.get('other_languages', [])
        if not isinstance(other_langs, list):
            other_langs = []
        
        # Combine all languages for this film
        all_languages = []
        if primary_lang:
            all_languages.append(primary_lang)
        all_languages.extend(other_langs)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_languages = []
        for lang in all_languages:
            if lang and lang not in seen:
                unique_languages.append(lang)
                seen.add(lang)
        
        # Count languages for each country this film is from
        for country in countries:
            if country:
                for language in unique_languages:
                    country_languages[country][language] += 1
    
    return dict(country_languages)

def get_percentage_threshold(lang_counts: dict, total_films: int) -> float:
    """
    Calculate a smart percentage threshold based on the country's language distribution.
    Uses purely percentage-based analysis to find natural break points in the data.
    
    Args:
        lang_counts: Dictionary of language -> count for this country
        total_films: Total number of films from the country
    
    Returns:
        Percentage threshold that captures the truly dominant languages
    """
    if not lang_counts or total_films == 0:
        return 100.0  # No languages meet this threshold, will fallback to top language
    
    # Sort languages by count (descending)
    sorted_langs = sorted(lang_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Calculate percentages
    percentages = [(lang, (count / total_films) * 100) for lang, count in sorted_langs]
    
    # If only one language, accept it regardless of percentage
    if len(percentages) == 1:
        return percentages[0][1]
    
    # Strategy: Find natural break points using percentage gaps
    
    # Find the biggest percentage gap between consecutive languages
    biggest_gap = 0
    best_threshold = percentages[0][1]
    
    for i in range(len(percentages) - 1):
        current_pct = percentages[i][1]
        next_pct = percentages[i + 1][1]
        gap = current_pct - next_pct
        
        # Consider this gap if it's significant relative to the current percentage
        # Gap should be at least 25% of the current language's percentage to be meaningful
        relative_gap = gap / current_pct if current_pct > 0 else 0
        
        if gap > biggest_gap and relative_gap >= 0.25:
            biggest_gap = gap
            best_threshold = next_pct
    
    # If no significant gap was found, use a percentage-based approach
    if biggest_gap == 0:
        top_percentage = percentages[0][1]
        
        # Use 40% of the top language's percentage as threshold
        # This ensures we only get languages that are reasonably significant
        best_threshold = top_percentage * 0.4
        
        # But don't go below 2% to avoid including very minor languages
        if best_threshold < 2.0 and top_percentage >= 5.0:
            best_threshold = 2.0
    
    return best_threshold

def create_language_mapping(country_languages: Dict[str, Dict[str, int]]) -> Dict[str, List[str]]:
    """
    Create a mapping of country to accepted languages based on actual usage with percentage-based thresholds.
    
    Args:
        country_languages: Country -> {language: count} mapping
    
    Returns:
        Dictionary mapping country -> list of accepted languages
    """
    country_language_mapping = {}
    
    print(f"\n🔍 Creating language mapping with adaptive percentage thresholds...")
    print("=" * 80)
    
    # Sort countries by film count (descending) to show major producers first
    countries_by_films = sorted(
        country_languages.items(), 
        key=lambda x: sum(x[1].values()), 
        reverse=True
    )
    
    for country, lang_counts in countries_by_films:
        total_films = sum(lang_counts.values())
        
        if total_films == 0:
            continue
        
        # Skip language filtering for countries with fewer than 50 films
        # These smaller countries are unlikely to have co-production issues
        if total_films < 50:
            print(f"\n{country} ({total_films} films) - Skipping language filtering (too few films)")
            country_language_mapping[country] = []  # Empty list means no filtering
            continue
        
        # Get adaptive percentage threshold based on this country's language distribution
        threshold_percentage = get_percentage_threshold(lang_counts, total_films)
        
        accepted_languages = []
        
        # Sort languages by count (most common first)
        sorted_langs = sorted(lang_counts.items(), key=lambda x: x[1], reverse=True)
        
        print(f"\n{country} ({total_films} films) - Adaptive threshold: {threshold_percentage:.1f}%:")
        
        for language, count in sorted_langs:
            percentage = (count / total_films) * 100
            
            # Accept language if it meets the adaptive threshold
            if percentage >= threshold_percentage:
                accepted_languages.append(language)
                print(f"  ✅ {language}: {count} films ({percentage:.1f}%)")
            else:
                print(f"  ❌ {language}: {count} films ({percentage:.1f}%)")
        
        # Always ensure we have at least the top language if none meet the threshold
        if not accepted_languages and sorted_langs:
            top_lang, top_count = sorted_langs[0]
            top_percentage = (top_count / total_films) * 100
            accepted_languages = [top_lang]
            print(f"  🎯 Taking top language (no threshold met): {top_lang} ({top_percentage:.1f}%)")
        
        country_language_mapping[country] = accepted_languages
    
    return country_language_mapping

def save_mapping_to_file(mapping: Dict[str, List[str]], output_file: str):
    """Save the language mapping to a Python file that can be imported."""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('"""Auto-generated country to languages mapping based on actual film data."""\n\n')
        f.write('# Country -> List of accepted languages\n')
        f.write('# Empty list means no language filtering (accept any language)\n')
        f.write('COUNTRY_LANGUAGES = {\n')
        
        for country, languages in sorted(mapping.items()):
            if languages:
                lang_list = ', '.join(f'"{lang}"' for lang in languages)
                f.write(f'    "{country}": [{lang_list}],\n')
            else:
                f.write(f'    "{country}": [],  # Any language accepted\n')
        
        f.write('}\n')
    
    print(f"\n💾 Language mapping saved to {output_file}")

def print_summary(country_languages: Dict[str, Dict[str, int]], 
                 mapping: Dict[str, List[str]]):
    """Print a summary of the analysis."""
    
    total_countries = len(country_languages)
    countries_with_filtering = len([c for c, langs in mapping.items() if langs])
    countries_without_filtering = total_countries - countries_with_filtering
    
    print(f"\n📊 SUMMARY")
    print("=" * 50)
    print(f"Total countries analyzed: {total_countries}")
    print(f"Countries with language filtering: {countries_with_filtering}")
    print(f"Countries without language filtering: {countries_without_filtering}")
    print(f"Language filtering coverage: {(countries_with_filtering/total_countries)*100:.1f}%")
    
    # Show breakdown by country size
    print(f"\n📈 Analysis by country film count:")
    
    country_sizes = []
    for country, lang_counts in country_languages.items():
        total_films = sum(lang_counts.values())
        has_filtering = len(mapping.get(country, [])) > 0
        country_sizes.append((country, total_films, has_filtering))
    
    # Sort by film count
    country_sizes.sort(key=lambda x: x[1], reverse=True)
    
    size_categories = [
        ("Large (1000+ films)", lambda x: x >= 1000),
        ("Medium (500-999 films)", lambda x: 500 <= x < 1000),
        ("Small-Medium (100-499 films)", lambda x: 100 <= x < 500),
        ("Small (50-99 films)", lambda x: 50 <= x < 100),
        ("Very Small (<50 films)", lambda x: x < 50)
    ]
    
    for category_name, category_filter in size_categories:
        category_countries = [(c, f, h) for c, f, h in country_sizes if category_filter(f)]
        if category_countries:
            filtered_count = sum(1 for _, _, has_filter in category_countries if has_filter)
            print(f"  {category_name}: {len(category_countries)} countries, {filtered_count} with filtering")
            
            # Show top countries in each category
            for country, films, has_filter in category_countries[:5]:
                status = "🎯" if has_filter else "🌍"
                lang_count = len(mapping.get(country, []))
                print(f"    {status} {country}: {films} films, {lang_count} languages accepted")
    
    # Show countries without filtering
    no_filter_countries = [c for c, langs in mapping.items() if not langs]
    if no_filter_countries:
        print(f"\n🌍 Countries without language filtering ({len(no_filter_countries)}):")
        for country in sorted(no_filter_countries)[:10]:  # Show first 10
            film_count = sum(country_languages[country].values())
            lang_count = len(country_languages[country])
            print(f"  {country}: {film_count} films, {lang_count} different languages")
        if len(no_filter_countries) > 10:
            print(f"  ... and {len(no_filter_countries) - 10} more")
    
    # Show most restrictive filtering
    restrictive_countries = [(c, langs) for c, langs in mapping.items() if 1 <= len(langs) <= 3]
    if restrictive_countries:
        print(f"\n🎯 Most focused language filtering:")
        restrictive_countries.sort(key=lambda x: len(x[1]))
        for country, languages in restrictive_countries[:10]:
            film_count = sum(country_languages[country].values())
            print(f"  {country}: {', '.join(languages)} ({film_count} films)")

def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_country_languages.py <detailed_films_file> [output_file]")
        print("  detailed_films_file: JSON file with detailed film data")
        print("  output_file: Output Python file (default: country_languages_percentage.py)")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "country_languages_percentage.py"
    
    # Load and analyze data
    films = load_film_data(input_file)
    country_languages = analyze_languages_by_country(films)
    
    # Create mapping with adaptive percentage thresholds
    mapping = create_language_mapping(country_languages)
    
    # Save the mapping
    save_mapping_to_file(mapping, output_file)
    
    # Print summary
    print_summary(country_languages, mapping)
    
    print(f"\n✅ Analysis complete! Use the generated {output_file} in your script.")
    print(f"💡 The new analysis uses adaptive percentage thresholds based on each country's language distribution.")
    print(f"🎯 This finds natural break points rather than using hard-coded thresholds.")

if __name__ == "__main__":
    main()
