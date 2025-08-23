"""
Pytest tests for the dynamic film extraction functionality.
Tests film data extraction and combination.
"""

import sys
import os
import pytest
import json
import logging

# Add the scrape_scripts directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from scrapers.letterboxd_scraper import LetterboxdScraper

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@pytest.fixture(scope="session")
def scraper():
    """Pytest fixture to create a scraper instance for all tests."""
    return LetterboxdScraper()


@pytest.fixture(scope="session")
def extracted_films(scraper):
    """Pytest fixture to extract films once and reuse across tests."""
    # Use the new approach: get predefined list info and use scraper methods directly
    username, list_slug = scraper.PREDEFINED_LISTS["my_top_100"]
    films = scraper.get_all_films_from_list_parallel(username, list_slug, max_workers=2)
    return films


@pytest.fixture(scope="session")
def sample_film_details(scraper, extracted_films):
    """Pytest fixture to get sample film details for reuse."""
    first_film = extracted_films[0]
    return scraper.get_film_details(first_film['film_slug'])


class TestFilmExtraction:
    """Test class for film extraction functionality."""
    
    @pytest.mark.network
    def test_extract_films_from_predefined_list(self, extracted_films):
        """Test extracting films from predefined list"""
        # Validate basic structure
        assert isinstance(extracted_films, list), "Films should be returned as a list"
        assert len(extracted_films) > 0, "List should contain films"
        assert len(extracted_films) <= 100, "List should not exceed 100 films"
        
        # Validate first film structure
        first_film = extracted_films[0]
        required_fields = ['film_id', 'film_slug', 'name', 'list_position']
        for field in required_fields:
            assert field in first_film, f"Film should have {field} field"
            assert first_film[field] is not None, f"{field} should not be None"
        
        # Validate list positions are sequential
        for i, film in enumerate(extracted_films[:5], 1):  # Check first 5
            assert film['list_position'] == str(i), f"Film {i} should have position {i}"
    
    @pytest.mark.network
    @pytest.mark.slow
    def test_extract_detailed_film_information(self, scraper, extracted_films, sample_film_details):
        """Test extracting detailed information for a specific film"""
        film_details = sample_film_details
        
        # Validate basic structure
        assert isinstance(film_details, dict), "Film details should be returned as a dict"
        
        # Validate essential fields
        essential_fields = ['title', 'year', 'director']
        for field in essential_fields:
            assert field in film_details, f"Film details should have {field} field"
            assert film_details[field] is not None, f"{field} should not be None"
        
        # Validate data types
        assert isinstance(film_details.get('cast', []), list), "Cast should be a list"
        assert isinstance(film_details.get('genres', []), list), "Genres should be a list"
        assert isinstance(film_details.get('countries', []), list), "Countries should be a list"
        assert isinstance(film_details.get('primary_language', ''), str), "Primary language should be a string"
        assert isinstance(film_details.get('other_languages', []), list), "Other languages should be a list"
        assert isinstance(film_details.get('studios', []), list), "Studios should be a list"
    
    @pytest.mark.network
    @pytest.mark.slow
    def test_create_combined_dataset(self, scraper, extracted_films):
        """Test creating combined dataset with film details"""
        combined_data = []
        test_film_count = 3  # Limit to 3 films for faster testing
        
        for film in extracted_films[:test_film_count]:
            film_details = scraper.get_film_details(film['film_slug'])
            
            # Combine list data with film details
            combined_film = {**film, **film_details}
            combined_data.append(combined_film)
        
        # Validate combined data structure
        assert len(combined_data) == test_film_count, f"Should have {test_film_count} combined films"
        
        for combined_film in combined_data:
            # Should have both list and detail fields
            assert 'list_position' in combined_film, "Should have list position"
            assert 'title' in combined_film, "Should have detailed title"
            assert 'director' in combined_film, "Should have director"
    
    @pytest.mark.network
    def test_data_consistency(self, scraper, extracted_films, sample_film_details):
        """Test data consistency between list and detail extraction"""
        first_film = extracted_films[0]
        film_details = sample_film_details
        
        # Name from list should match title from details (allowing for minor variations)
        list_name = first_film['name'].lower().strip()
        detail_title = film_details['title'].lower().strip()
        
        # They should be similar (exact match or one contains the other)
        assert (list_name == detail_title or 
                list_name in detail_title or 
                detail_title in list_name), \
               f"List name '{first_film['name']}' should match detail title '{film_details['title']}'"
    
    @pytest.mark.network
    def test_error_handling_invalid_film(self, scraper):
        """Test error handling with invalid film slug"""
        with pytest.raises(Exception):
            scraper.get_film_details("non-existent-film-slug-12345")
    
    def test_error_handling_invalid_list(self, scraper):
        """Test error handling with invalid predefined list"""
        # Test that invalid predefined list key raises appropriate error
        with pytest.raises(KeyError):
            invalid_key = "non_existent_list"
            username, list_slug = scraper.PREDEFINED_LISTS[invalid_key]


class TestFilmExtractionIntegration:
    """Integration tests for complete workflows."""
    
    @pytest.mark.integration
    @pytest.mark.slow
    @pytest.mark.network
    def test_complete_workflow(self, scraper):
        """Test complete workflow from list extraction to data export"""
        # Extract a small subset using the new approach
        username, list_slug = scraper.PREDEFINED_LISTS["my_top_100"]
        films = scraper.get_all_films_from_list_parallel(username, list_slug, max_workers=2)
        sample_films = films[:2]  # Just 2 films for quick test
        
        # Get details for each
        detailed_films = []
        for film in sample_films:
            details = scraper.get_film_details(film['film_slug'])
            combined = {**film, **details}
            detailed_films.append(combined)
        
        # Validate the complete workflow
        assert len(detailed_films) == 2, "Should have 2 detailed films"
        
        for film in detailed_films:
            # Should have all essential data
            essential_keys = ['list_position', 'name', 'title', 'year', 'director']
            for key in essential_keys:
                assert key in film, f"Film should have {key}"


# Test data persistence and cleanup
@pytest.fixture(scope="session", autouse=True)
def test_data_cleanup():
    """Automatically clean up test files after test session."""
    test_files = []
    
    # Yield control to tests
    yield test_files
    
    # Cleanup after all tests
    cleanup_files = [
        "test_extracted_films.json",
        "test_film_details_*.json", 
        "test_combined_film_data.json"
    ]
    
    for pattern in cleanup_files:
        if '*' in pattern:
            # Handle wildcard patterns
            import glob
            for file_path in glob.glob(pattern):
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as e:
                    print(f"Warning: Could not remove test file {file_path}: {e}")
        else:
            try:
                if os.path.exists(pattern):
                    os.remove(pattern)
            except Exception as e:
                print(f"Warning: Could not remove test file {pattern}: {e}")


@pytest.mark.unit
def test_scraper_initialization():
    """Test that scraper can be initialized without network calls."""
    scraper = LetterboxdScraper()
    assert scraper is not None
    assert hasattr(scraper, 'letterboxd_session')
    assert hasattr(scraper, 'PREDEFINED_LISTS')
