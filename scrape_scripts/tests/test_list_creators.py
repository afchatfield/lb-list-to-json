import pytest
import json
from pathlib import Path
import sys
import re
import glob
import os
import logging
import tempfile

# Add the project root to the Python path to allow for absolute imports
ROOT_PATH = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT_PATH))

from scrape_scripts.list_creators import ListCreator, ListConfig, SortBy

@pytest.fixture(scope="module")
def all_json_files():
    """Fixture to get the latest 'all_the_films_detailed' JSON file."""
    output_dir = ROOT_PATH / 'scrape_scripts' / 'output'
    list_of_files = glob.glob(str(output_dir / 'all_the_films_detailed_*.json'))
    if not list_of_files:
        pytest.fail("No 'all_the_films_detailed' JSON files found in scrape_scripts/output")
    latest_file = max(list_of_files, key=os.path.getctime)
    return [latest_file]

@pytest.fixture(scope="module")
def stats_file():
    """Fixture to get the latest stats file."""
    output_dir = ROOT_PATH / 'scrape_scripts' / 'output'
    list_of_files = glob.glob(str(output_dir / 'all_the_films_ratings_stats_*.json'))
    if not list_of_files:
        return None  # It's optional
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

@pytest.fixture(scope="module")
def list_creator(all_json_files, stats_file):
    """Fixture to create a ListCreator instance with the correct JSON files."""
    files_list = all_json_files[:]
    if stats_file:
        files_list.append(stats_file)
    return ListCreator(json_files=files_list)

@pytest.fixture(scope="module")
def reference_japanese_films():
    """Fixture to load the reference list of top 250 Japanese films."""
    reference_file = ROOT_PATH / 'ratings' / 'top_250_japanese_films.json'
    with open(reference_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_create_top_250_japanese_films_list(list_creator, reference_japanese_films, request):
    """
    Test creating a list of top 250 Japanese films in Japanese and compare it
    with an existing reference JSON file. The test checks for a high percentage
    of overlap to validate the dynamic list creation.
    """
    # 1. Define the configuration for the list to be created
    config = ListConfig(
        title="Top 250 Japanese Films (Japanese Language)",
        description="A dynamically generated list of top Japanese films.",
        limit=250,
        sort_by=SortBy.AVERAGE_RATING,
        sort_ascending=False
    )

    # 2. Generate the list using the ListCreator with ratings cutoff
    config.countries = ['Japan']
    config.languages = ['Japanese'] 
    config.cutoff_type = 'ratings'
    config.cutoff_limit = 2000
    
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
        tmp_output_path = Path(tmp_file.name)
    
    try:
        generated_list = list_creator.create_list(
            config=config,
            output_path=tmp_output_path
        )
    finally:
        # Clean up temp file
        if tmp_output_path.exists():
            tmp_output_path.unlink()

    # 3. Assert basic properties of the generated list
    assert generated_list['title'] == config.title
    assert len(generated_list['films']) <= config.limit
    assert generated_list['films_returned'] > 0, "The generated list should not be empty."

    # 4. Compare the generated list with the reference list
    def normalize_title(title):
        """Converts title to lowercase and removes non-alphanumeric characters."""
        return re.sub(r'[^a-z0-9]+', '', title.lower())

    reference_film_titles = {normalize_title(film['Name']) for film in reference_japanese_films}
    generated_film_titles = {normalize_title(film['name']) for film in generated_list['films']}

    # Calculate the overlap between the two sets of film titles
    overlap_count = len(reference_film_titles.intersection(generated_film_titles))
    
    if not reference_film_titles:
        pytest.fail("Reference film ID set is empty, cannot perform comparison.")

    overlap_percentage = (overlap_count / len(reference_film_titles)) * 100

    # 5. Assert that the overlap is at least 80%
    if request.config.getoption("verbose"):
        percentage_difference = 100 - overlap_percentage
        logging.info(f"Overlap with reference list: {overlap_percentage:.2f}%")
        logging.info(f"Percentage difference with reference list: {percentage_difference:.2f}%")

    if overlap_percentage < 80.0:
        unique_to_reference = reference_film_titles - generated_film_titles
        unique_to_generated = generated_film_titles - reference_film_titles
        print("\n--- Debugging Info ---")
        print(f"Found {len(unique_to_reference)} titles in reference not in generated list.")
        # Print a few examples
        for title in list(unique_to_reference)[:5]:
            print(f"  - Ref only: {title}")

        print(f"\nFound {len(unique_to_generated)} titles in generated list not in reference.")
        for title in list(unique_to_generated)[:5]:
            print(f"  - Gen only: {title}")
        print("--- End Debugging Info ---\n")

    assert overlap_percentage >= 80.0, (
        f"Generated list has only {overlap_percentage:.2f}% overlap with the "
        f"reference list, which is below the 80% threshold."
    )


def test_cutoff_ratings_functionality(list_creator):
    """
    Test the cutoff functionality based on ratings count.
    """
    # Create a config for testing
    config = ListConfig(
        title="Test Ratings Cutoff List",
        description="Testing minimum ratings cutoff functionality.",
        limit=100,
        sort_by=SortBy.AVERAGE_RATING,
        sort_ascending=False
    )

    # Generate list without cutoff
    config_without_cutoff = ListConfig(
        title="Test Without Cutoff",
        description="Testing without cutoff functionality.",
        limit=100,
        sort_by=SortBy.AVERAGE_RATING,
        sort_ascending=False,
        countries=['Japan'],
        languages=['Japanese']
    )
    
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
        tmp_output_path1 = Path(tmp_file.name)
    
    try:
        list_without_cutoff = list_creator.create_list(
            config=config_without_cutoff,
            output_path=tmp_output_path1
        )
    finally:
        if tmp_output_path1.exists():
            tmp_output_path1.unlink()

    # Generate list with ratings cutoff
    config_with_cutoff = ListConfig(
        title="Test With Cutoff",
        description="Testing with cutoff functionality.",
        limit=100,
        sort_by=SortBy.AVERAGE_RATING,
        sort_ascending=False,
        countries=['Japan'],
        languages=['Japanese'],
        cutoff_type='ratings',
        cutoff_limit=1000
    )
    
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
        tmp_output_path2 = Path(tmp_file.name)
    
    try:
        list_with_cutoff = list_creator.create_list(
            config=config_with_cutoff,
            output_path=tmp_output_path2
        )
    finally:
        if tmp_output_path2.exists():
            tmp_output_path2.unlink()

    # Verify that cutoff filters reduce the number of films
    assert list_with_cutoff['total_found'] <= list_without_cutoff['total_found']
    assert list_with_cutoff['films_returned'] <= list_without_cutoff['films_returned']

    # Verify cutoff type and limit are recorded in filters
    assert list_with_cutoff['filters']['cutoff_type'] == 'ratings'
    assert list_with_cutoff['filters']['cutoff_limit'] == 1000

    print(f"Without cutoff: {list_without_cutoff['total_found']} films found")
    print(f"With ratings cutoff (1000): {list_with_cutoff['total_found']} films found")


def test_cutoff_watches_functionality(list_creator):
    """
    Test the cutoff functionality based on watches count.
    """
    # Create a config for testing
    config = ListConfig(
        title="Test Watches Cutoff List",
        description="Testing minimum watches cutoff functionality.",
        limit=50,
        sort_by=SortBy.AVERAGE_RATING,
        sort_ascending=False
    )

    # Generate list without cutoff
    config_without_cutoff = ListConfig(
        title="Test Without Cutoff",
        description="Testing without cutoff functionality.",
        limit=50,
        sort_by=SortBy.AVERAGE_RATING,
        sort_ascending=False,
        countries=['USA'],
        languages=['English']
    )
    
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
        tmp_output_path1 = Path(tmp_file.name)
    
    try:
        list_without_cutoff = list_creator.create_list(
            config=config_without_cutoff,
            output_path=tmp_output_path1
        )
    finally:
        if tmp_output_path1.exists():
            tmp_output_path1.unlink()

    # Generate list with watches cutoff
    config_with_cutoff = ListConfig(
        title="Test With Cutoff",
        description="Testing with cutoff functionality.",
        limit=50,
        sort_by=SortBy.AVERAGE_RATING,
        sort_ascending=False,
        countries=['USA'],
        languages=['English'],
        cutoff_type='watches',
        cutoff_limit=5000
    )
    
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
        tmp_output_path2 = Path(tmp_file.name)
    
    try:
        list_with_cutoff = list_creator.create_list(
            config=config_with_cutoff,
            output_path=tmp_output_path2
        )
    finally:
        if tmp_output_path2.exists():
            tmp_output_path2.unlink()

    # Verify that cutoff filters reduce the number of films
    assert list_with_cutoff['total_found'] <= list_without_cutoff['total_found']
    assert list_with_cutoff['films_returned'] <= list_without_cutoff['films_returned']

    # Verify cutoff type and limit are recorded in filters
    assert list_with_cutoff['filters']['cutoff_type'] == 'watches'
    assert list_with_cutoff['filters']['cutoff_limit'] == 5000

    print(f"Without cutoff: {list_without_cutoff['total_found']} films found")
    print(f"With watches cutoff (5000): {list_with_cutoff['total_found']} films found")


def test_cutoff_validation_with_sample_data():
    """
    Test cutoff functionality with controlled sample data.
    """
    # Create sample data with known ratings and watches counts
    sample_films = [
        {
            'film_id': '1',
            'name': 'High Rating High Count',
            'average_rating': 4.5,
            'ratings_count': 5000,
            'watches_count': 10000,
            'watches_count_exact': 10000,
            'countries': ['USA'],
            'primary_language': 'English'
        },
        {
            'film_id': '2',
            'name': 'High Rating Low Count',
            'average_rating': 4.3,
            'ratings_count': 500,
            'watches_count': 800,
            'watches_count_exact': 800,
            'countries': ['USA'],
            'primary_language': 'English'
        },
        {
            'film_id': '3',
            'name': 'Medium Rating Medium Count',
            'average_rating': 3.8,
            'ratings_count': 2000,
            'watches_count': 3000,
            'watches_count_exact': 3000,
            'countries': ['USA'],
            'primary_language': 'English'
        }
    ]

    # Create a temporary JSON file with sample data
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        json.dump(sample_films, temp_file)
        temp_file_path = temp_file.name

    try:
        # Create ListCreator with sample data
        test_creator = ListCreator(json_files=[temp_file_path])

        config = ListConfig(
            title="Test Sample Data",
            description="Testing with controlled sample data.",
            limit=10,
            sort_by=SortBy.AVERAGE_RATING,
            sort_ascending=False
        )

        # Test ratings cutoff of 1000 (should filter out film 2)
        config_ratings = ListConfig(
            title="Test Sample Data Ratings",
            description="Testing with controlled sample data.",
            limit=10,
            sort_by=SortBy.AVERAGE_RATING,
            sort_ascending=False,
            cutoff_type='ratings',
            cutoff_limit=1000
        )
        
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
            tmp_output_path1 = Path(tmp_file.name)
        
        try:
            list_ratings_cutoff = test_creator.create_list(
                config=config_ratings,
                output_path=tmp_output_path1
            )
        finally:
            if tmp_output_path1.exists():
                tmp_output_path1.unlink()

        # Should only include films 1 and 3
        assert list_ratings_cutoff['films_returned'] == 2
        film_names = [film['name'] for film in list_ratings_cutoff['films']]
        assert 'High Rating High Count' in film_names
        assert 'Medium Rating Medium Count' in film_names
        assert 'High Rating Low Count' not in film_names

        # Test watches cutoff of 2000 (should filter out film 2)
        config_watches = ListConfig(
            title="Test Sample Data Watches",
            description="Testing with controlled sample data.",
            limit=10,
            sort_by=SortBy.AVERAGE_RATING,
            sort_ascending=False,
            cutoff_type='watches',
            cutoff_limit=2000
        )
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
            tmp_output_path2 = Path(tmp_file.name)
        
        try:
            list_watches_cutoff = test_creator.create_list(
                config=config_watches,
                output_path=tmp_output_path2
            )
        finally:
            if tmp_output_path2.exists():
                tmp_output_path2.unlink()

        # Should only include films 1 and 3
        assert list_watches_cutoff['films_returned'] == 2
        film_names = [film['name'] for film in list_watches_cutoff['films']]
        assert 'High Rating High Count' in film_names
        assert 'Medium Rating Medium Count' in film_names
        assert 'High Rating Low Count' not in film_names

    finally:
        # Clean up temporary file
        import os
        os.unlink(temp_file_path)
