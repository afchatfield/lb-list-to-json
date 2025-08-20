"""
Pytest tests for the Letterboxd DataFrame parser functionality.
Tests data conversion, cleaning, and analysis capabilities.
"""

import sys
import os
import pytest
import pandas as pd
import json
from unittest.mock import patch

# Add the scrape_scripts directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from parsers.letterboxd_parser import (
    LetterboxdParser, FilmDataFrameBuilder,
    create_letterboxd_dataframe, create_summary_dataframe, create_genre_analysis_dataframe
)


@pytest.fixture
def sample_film_data():
    """Sample film data for testing."""
    return [
        {
            "film_id": "426406",
            "film_slug": "parasite-2019",
            "name": "Parasite",
            "title": "Parasite",
            "year": "2019",
            "director": "Bong Joon Ho",
            "list_position": "1",
            "owner_rating": "10",
            "genres": ["Thriller", "Comedy", "Drama"],
            "countries": ["South Korea"],
            "cast": ["Song Kang-ho", "Lee Sun-kyun", "Cho Yeo-jeong"],
            "runtime": "132 mins",
            "target_link": "/film/parasite-2019/"
        },
        {
            "film_id": "18627",
            "film_slug": "incendies",
            "name": "Incendies",
            "title": "Incendies",
            "year": "2010",
            "director": "Denis Villeneuve",
            "list_position": "2",
            "owner_rating": "9.5",
            "genres": ["Drama", "Mystery", "War"],
            "countries": ["Canada", "France"],
            "cast": ["Lubna Azabal", "Mélissa Désormeaux-Poulin"],
            "runtime": "2h 11m",
            "target_link": "/film/incendies/"
        }
    ]


@pytest.fixture
def parser():
    """Fixture for LetterboxdParser instance."""
    return LetterboxdParser()


@pytest.fixture
def builder():
    """Fixture for FilmDataFrameBuilder instance."""
    return FilmDataFrameBuilder()


class TestLetterboxdParser:
    """Test class for LetterboxdParser functionality."""
    
    def test_parser_initialization(self, parser):
        """Test parser initialization."""
        assert parser.clean_data == True
        assert parser.drop_duplicates == True
        assert parser.validate_data == True
        assert 'rating' in parser.cleaners
        assert 'runtime' in parser.cleaners
    
    def test_parse_to_dataframe_basic(self, parser, sample_film_data):
        """Test basic DataFrame creation."""
        df = parser.parse_to_dataframe(sample_film_data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'name' in df.columns
        assert 'film_slug' in df.columns
    
    def test_parse_empty_data(self, parser):
        """Test parsing empty data."""
        df = parser.parse_to_dataframe([])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
    
    def test_clean_rating_string(self, parser):
        """Test rating cleaning with string input."""
        assert parser._clean_rating("10") == 10.0
        assert parser._clean_rating("9.5") == 9.5
        assert parser._clean_rating("★★★★★") == 5.0
        assert parser._clean_rating("★★★★☆") == 4.0
        assert parser._clean_rating("★★★½") == 3.5
        assert parser._clean_rating("invalid") is None
    
    def test_clean_rating_numeric(self, parser):
        """Test rating cleaning with numeric input."""
        assert parser._clean_rating(10) == 10.0
        assert parser._clean_rating(5.0) == 10.0  # Convert from 5-star to 10-point
        assert parser._clean_rating(2.5) == 5.0
        assert parser._clean_rating(15) is None  # Invalid range
    
    def test_clean_runtime(self, parser):
        """Test runtime cleaning."""
        assert parser._clean_runtime("132 mins") == 132
        assert parser._clean_runtime("2h 11m") == 131
        assert parser._clean_runtime("1h 30m") == 90
        assert parser._clean_runtime("45m") == 45
        assert parser._clean_runtime("2h") == 120
        assert parser._clean_runtime("invalid") is None
    
    def test_clean_genre_list_string(self, parser):
        """Test genre list cleaning from string."""
        result = parser._clean_genre_list("Action, Comedy, Drama")
        assert result == ["Action", "Comedy", "Drama"]
        
        result = parser._clean_genre_list("Thriller;Mystery;Horror")
        assert result == ["Thriller", "Mystery", "Horror"]
        
        result = parser._clean_genre_list("")
        assert result == []
    
    def test_clean_genre_list_array(self, parser):
        """Test genre list cleaning from array."""
        result = parser._clean_genre_list(["Action", "Comedy", "Drama"])
        assert result == ["Action", "Comedy", "Drama"]
        
        result = parser._clean_genre_list([])
        assert result == []
    
    def test_clean_cast_list(self, parser):
        """Test cast list cleaning."""
        result = parser._clean_cast_list("Actor One, Actor Two, Actor Three")
        assert result == ["Actor One", "Actor Two", "Actor Three"]
        
        result = parser._clean_cast_list(["Actor One", "Actor Two"])
        assert result == ["Actor One", "Actor Two"]
    
    def test_clean_film_id(self, parser):
        """Test film ID cleaning."""
        assert parser._clean_film_id("426406") == 426406
        assert parser._clean_film_id(426406) == 426406
        assert parser._clean_film_id("id-426406") == 426406
        assert parser._clean_film_id("invalid") is None
    
    def test_clean_list_position(self, parser):
        """Test list position cleaning."""
        assert parser._clean_list_position("1") == 1
        assert parser._clean_list_position(1) == 1
        assert parser._clean_list_position("#1") == 1
        assert parser._clean_list_position("position-5") == 5
        assert parser._clean_list_position("invalid") is None
    
    def test_standardize_genre(self, parser):
        """Test genre standardization."""
        assert parser._standardize_genre("sci-fi") == "Science Fiction"
        assert parser._standardize_genre("rom-com") == "Romantic Comedy"
        assert parser._standardize_genre("action") == "Action"
    
    def test_standardize_country(self, parser):
        """Test country standardization."""
        assert parser._standardize_country("usa") == "United States"
        assert parser._standardize_country("uk") == "United Kingdom"
        assert parser._standardize_country("south korea") == "South Korea"
        assert parser._standardize_country("France") == "France"


class TestFilmDataFrameBuilder:
    """Test class for FilmDataFrameBuilder functionality."""
    
    def test_build_films_dataframe(self, builder, sample_film_data):
        """Test building films DataFrame."""
        df = builder.build_films_dataframe(sample_film_data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'decade' in df.columns
        assert 'has_rating' in df.columns
        assert 'genre_count' in df.columns
    
    def test_build_list_summary_dataframe(self, builder, sample_film_data):
        """Test building summary DataFrame."""
        summary_df = builder.build_list_summary_dataframe(sample_film_data)
        
        assert isinstance(summary_df, pd.DataFrame)
        assert len(summary_df) > 0
        assert 'metric' in summary_df.columns
        assert 'value' in summary_df.columns
        assert 'description' in summary_df.columns
    
    def test_build_genre_analysis_dataframe(self, builder, sample_film_data):
        """Test building genre analysis DataFrame."""
        genre_df = builder.build_genre_analysis_dataframe(sample_film_data)
        
        assert isinstance(genre_df, pd.DataFrame)
        assert len(genre_df) > 0
        assert 'genre' in genre_df.columns
        assert 'count' in genre_df.columns
        assert 'percentage' in genre_df.columns
    
    def test_add_computed_columns(self, builder, sample_film_data):
        """Test adding computed columns."""
        df = pd.DataFrame(sample_film_data)
        df = builder._add_computed_columns(df)
        
        assert 'decade' in df.columns
        assert df.loc[0, 'decade'] == 2010  # 2019 -> 2010
        assert df.loc[1, 'decade'] == 2010  # 2010 -> 2010
        
        assert 'has_rating' in df.columns
        assert df.loc[0, 'has_rating'] == True
        
        assert 'genre_count' in df.columns
        assert df.loc[0, 'genre_count'] == 3


class TestConvenienceFunctions:
    """Test class for convenience functions."""
    
    def test_create_letterboxd_dataframe(self, sample_film_data):
        """Test convenience function for creating DataFrame."""
        df = create_letterboxd_dataframe(sample_film_data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'decade' in df.columns
    
    def test_create_summary_dataframe(self, sample_film_data):
        """Test convenience function for creating summary."""
        summary_df = create_summary_dataframe(sample_film_data)
        
        assert isinstance(summary_df, pd.DataFrame)
        assert len(summary_df) > 0
    
    def test_create_genre_analysis_dataframe(self, sample_film_data):
        """Test convenience function for creating genre analysis."""
        genre_df = create_genre_analysis_dataframe(sample_film_data)
        
        assert isinstance(genre_df, pd.DataFrame)
        assert len(genre_df) > 0


class TestDataValidation:
    """Test class for data validation functionality."""
    
    def test_validate_content_valid_data(self, parser, sample_film_data):
        """Test validation with valid data."""
        df = pd.DataFrame(sample_film_data)
        
        # Should not raise any exceptions
        parser.validate_content(df)
    
    def test_validate_content_invalid_years(self, parser):
        """Test validation with invalid years."""
        invalid_data = [{
            "name": "Test Film",
            "film_slug": "test-film",
            "year": 1700  # Too old
        }]
        
        df = pd.DataFrame(invalid_data)
        
        with patch('parsers.letterboxd_parser.logger') as mock_logger:
            parser.validate_content(df)
            mock_logger.warning.assert_called()
    
    def test_validate_content_invalid_ratings(self, parser):
        """Test validation with invalid ratings."""
        invalid_data = [{
            "name": "Test Film",
            "film_slug": "test-film",
            "owner_rating": 15  # Too high
        }]
        
        df = pd.DataFrame(invalid_data)
        
        with patch('parsers.letterboxd_parser.logger') as mock_logger:
            parser.validate_content(df)
            mock_logger.warning.assert_called()


class TestEdgeCases:
    """Test class for edge cases and error handling."""
    
    def test_parse_none_values(self, parser):
        """Test parsing with None values."""
        data_with_nones = [{
            "name": "Test Film",
            "film_slug": "test-film",
            "year": None,
            "director": None,
            "owner_rating": None
        }]
        
        df = parser.parse_to_dataframe(data_with_nones)
        
        assert len(df) == 1
        assert pd.isna(df.loc[0, 'year'])
    
    def test_parse_mixed_data_types(self, parser):
        """Test parsing with mixed data types."""
        mixed_data = [{
            "name": "Test Film",
            "film_slug": "test-film",
            "year": "2020",  # String year
            "owner_rating": 5,  # Integer rating
            "genres": "Action, Comedy",  # String genres
        }]
        
        df = parser.parse_to_dataframe(mixed_data)
        
        assert len(df) == 1
        assert df.loc[0, 'year'] == 2020  # Should be converted to int
    
    def test_duplicate_removal(self, parser):
        """Test duplicate removal functionality."""
        duplicate_data = [
            {"name": "Film 1", "film_slug": "film-1"},
            {"name": "Film 1", "film_slug": "film-1"},  # Duplicate
            {"name": "Film 2", "film_slug": "film-2"}
        ]
        
        df = parser.parse_to_dataframe(duplicate_data)
        
        assert len(df) == 2  # Should remove duplicate
    
    def test_large_dataset_performance(self, parser):
        """Test performance with larger dataset."""
        # Create a larger dataset
        large_data = []
        for i in range(1000):
            large_data.append({
                "name": f"Film {i}",
                "film_slug": f"film-{i}",
                "year": "2020",
                "director": f"Director {i}",
                "owner_rating": "8.5"
            })
        
        # This should complete reasonably quickly
        df = parser.parse_to_dataframe(large_data)
        
        assert len(df) == 1000
        assert 'name' in df.columns


@pytest.mark.integration
class TestDataProcessingIntegration:
    """Integration tests for complete data processing workflows."""
    
    def test_full_workflow(self, sample_film_data):
        """Test complete workflow from JSON to analysis."""
        # Create DataFrame
        df = create_letterboxd_dataframe(sample_film_data)
        assert len(df) == 2
        
        # Create summary
        summary_df = create_summary_dataframe(sample_film_data)
        assert len(summary_df) > 0
        
        # Create genre analysis
        genre_df = create_genre_analysis_dataframe(sample_film_data)
        assert len(genre_df) > 0
        
        # All should be DataFrames
        assert all(isinstance(df, pd.DataFrame) for df in [df, summary_df, genre_df])
