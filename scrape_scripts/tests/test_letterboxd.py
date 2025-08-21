"""
Pytest tests for the Letterboxd scraper.
Tests connection and basic scraping functionality.
"""

import sys
import os
import pytest
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


@pytest.mark.network
def test_connection(scraper):
    """Test connection to Letterboxd.com"""
    assert scraper.test_connection(), "Failed to establish connection to Letterboxd.com"


@pytest.mark.network
def test_homepage_retrieval(scraper):
    """Test homepage retrieval"""
    homepage_soup = scraper.get_page_soup("/")
    assert homepage_soup is not None, "Homepage soup should not be None"
    
    title = homepage_soup.find("title")
    assert title is not None, "Homepage should have a title tag"
    assert "Letterboxd" in title.text, "Title should contain 'Letterboxd'"


@pytest.mark.network
def test_film_page_retrieval(scraper):
    """Test film page retrieval"""
    film_soup = scraper.get_film_soup("the-phoenician-scheme")
    assert film_soup is not None, "Film soup should not be None"
    
    # Look for film title
    film_title = film_soup.find("h1", class_="headline-1")
    assert film_title is not None, "Film page should have a headline-1 title"


@pytest.mark.network
def test_list_page_retrieval(scraper):
    """Test list page retrieval"""
    list_soup = scraper.get_list_soup("el_duderinno", "my-top-100")
    assert list_soup is not None, "List soup should not be None"
    
    # Check for list title
    list_title = list_soup.find("h1", class_="title-1")
    assert list_title is not None, "List page should have a title-1 heading"
    
    # Check for film items
    film_items = list_soup.find_all("li", class_="poster-container")
    assert len(film_items) > 0, "List should contain film items"
    assert len(film_items) <= 100, "List should not exceed 100 items"


@pytest.mark.network
def test_predefined_list_helper(scraper):
    """Test predefined list helper function using the new CLI approach"""
    # Test that the scraper has predefined lists configured
    assert hasattr(scraper, 'PREDEFINED_LISTS'), "Scraper should have PREDEFINED_LISTS"
    assert "my_top_100" in scraper.PREDEFINED_LISTS, "Should have my_top_100 predefined list"
    
    # Get the predefined list info
    username, list_slug = scraper.PREDEFINED_LISTS["my_top_100"]
    
    # Test that we can get basic films from the predefined list
    predefined_films = scraper.get_all_films_from_list_parallel(username, list_slug, max_workers=2)
    assert predefined_films is not None, "Predefined list films should not be None"
    assert len(predefined_films) > 0, "Predefined list should contain films"
    
    # Check that the first film has expected properties
    first_film = predefined_films[0]
    assert 'name' in first_film, "Film should have a name"
    assert 'film_slug' in first_film, "Film should have a film_slug"
