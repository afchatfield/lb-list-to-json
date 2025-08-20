"""
Base parser class for converting scraped HTML data to pandas DataFrames.
Provides common functionality for data cleaning and transformation.
"""

import pandas as pd
import re
import logging
from typing import Dict, List, Any, Optional, Callable
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """
    Abstract base class for data parsers.
    Converts scraped data to pandas DataFrames with cleaning and validation.
    """
    
    def __init__(self, 
                 clean_data: bool = True,
                 drop_duplicates: bool = True,
                 validate_data: bool = True):
        """
        Initialize the base parser.
        
        Args:
            clean_data: Whether to apply data cleaning
            drop_duplicates: Whether to remove duplicate rows
            validate_data: Whether to validate data after parsing
        """
        self.clean_data = clean_data
        self.drop_duplicates = drop_duplicates
        self.validate_data = validate_data
        
        # Common cleaning functions
        self.cleaners = {
            'text': self._clean_text,
            'year': self._clean_year,
            'url': self._clean_url,
            'numeric': self._clean_numeric
        }
    
    def parse_to_dataframe(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert list of dictionaries to pandas DataFrame.
        
        Args:
            data: List of dictionaries containing scraped data
            
        Returns:
            Cleaned and validated DataFrame
        """
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        
        if self.clean_data:
            df = self._apply_cleaning(df)
        
        if self.drop_duplicates:
            # Handle list columns for duplicate detection
            df = self._drop_duplicates_with_lists(df)
        
        if self.validate_data:
            self._validate_dataframe(df)
        
        return df
    
    def _apply_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply cleaning functions to DataFrame columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        # Get column cleaning rules from subclass
        cleaning_rules = self.get_cleaning_rules()
        
        for column, cleaner_type in cleaning_rules.items():
            if column in df.columns and cleaner_type in self.cleaners:
                def safe_clean(x):
                    # Handle None/NaN values
                    if x is None or (isinstance(x, float) and pd.isna(x)):
                        return x
                    # Check if it's already in the correct format (like a list)
                    if cleaner_type in ['genre_list', 'cast_list', 'country_list'] and isinstance(x, list):
                        return x
                    return self.cleaners[cleaner_type](x)
                
                df[column] = df[column].apply(safe_clean)
        
        return df
    
    def _clean_text(self, text: str) -> str:
        """Clean text data by removing extra whitespace and special characters."""
        if not isinstance(text, str):
            return str(text) if text is not None else ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove special characters that might cause issues
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        return text
    
    def _clean_year(self, year: str) -> Optional[int]:
        """Extract and validate year from text."""
        if not isinstance(year, str):
            return None
        
        # Extract 4-digit year
        year_match = re.search(r'\b(19|20)\d{2}\b', year)
        if year_match:
            return int(year_match.group())
        
        return None
    
    def _clean_url(self, url: str) -> str:
        """Clean and validate URL."""
        if not isinstance(url, str):
            return ""
        
        # Remove extra whitespace
        url = url.strip()
        
        # Ensure proper URL format
        if url and not url.startswith(('http://', 'https://')):
            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                url = 'https://letterboxd.com' + url
        
        return url
    
    def _clean_numeric(self, value: str) -> Optional[float]:
        """Extract numeric value from text."""
        if not isinstance(value, str):
            return None
        
        # Extract number (including decimals)
        number_match = re.search(r'\d+\.?\d*', value)
        if number_match:
            return float(number_match.group())
        
        return None
    
    def _drop_duplicates_with_lists(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drop duplicates handling list columns by converting them to strings temporarily.
        
        Args:
            df: DataFrame to deduplicate
            
        Returns:
            DataFrame with duplicates removed
        """
        if df.empty:
            return df
        
        # Create a copy for deduplication
        temp_df = df.copy()
        
        # Convert list columns to strings for duplicate detection
        list_columns = []
        for col in temp_df.columns:
            if temp_df[col].dtype == 'object':
                # Check if column contains lists
                sample_non_null = temp_df[col].dropna()
                if not sample_non_null.empty and isinstance(sample_non_null.iloc[0], list):
                    list_columns.append(col)
                    temp_df[col] = temp_df[col].apply(lambda x: str(sorted(x)) if isinstance(x, list) else x)
        
        # Drop duplicates using the temporary DataFrame
        mask = ~temp_df.duplicated()
        result_df = df[mask].reset_index(drop=True)
        
        return result_df
    
    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """
        Validate DataFrame structure and content.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ValueError: If validation fails
        """
        required_columns = self.get_required_columns()
        
        # Check required columns exist
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for empty DataFrame
        if df.empty:
            raise ValueError("DataFrame is empty after parsing")
        
        # Additional validation from subclass
        self.validate_content(df)
    
    def add_custom_cleaner(self, name: str, cleaner_func: Callable[[str], Any]) -> None:
        """
        Add custom cleaning function.
        
        Args:
            name: Name of the cleaner
            cleaner_func: Function that takes a string and returns cleaned value
        """
        self.cleaners[name] = cleaner_func
    
    @abstractmethod
    def get_cleaning_rules(self) -> Dict[str, str]:
        """
        Return dictionary mapping column names to cleaner types.
        Must be implemented by subclasses.
        
        Returns:
            Dictionary of column_name: cleaner_type
        """
        pass
    
    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """
        Return list of required column names.
        Must be implemented by subclasses.
        
        Returns:
            List of required column names
        """
        pass
    
    @abstractmethod
    def validate_content(self, df: pd.DataFrame) -> None:
        """
        Perform content-specific validation.
        Must be implemented by subclasses.
        
        Args:
            df: DataFrame to validate
        """
        pass
