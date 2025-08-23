#!/usr/bin/env python3
"""
Selector validation script.
Tests that all selectors in the letterboxd_selectors.json are properly loaded and accessible.
"""

import sys
import os
import json
from pathlib import Path

# Add parent directory to path for imports
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from core.config_loader import selector_config, get_selectors, get_selector


def main():
    """Test selector loading and access."""
    print("🎬 Letterboxd Selector Validation")
    print("=" * 50)
    
    # Test 1: Load complete configuration
    print("Test 1: Loading complete selector configuration...")
    selectors = get_selectors()
    
    if selectors:
        print("✅ Selectors loaded successfully")
        print(f"   Found {len(selectors)} main categories:")
        for category in selectors.keys():
            print(f"   - {category}")
    else:
        print("❌ Failed to load selectors")
        return False
    
    # Test 2: Test specific selector access
    print("\nTest 2: Testing specific selector access...")
    test_selectors = [
        "film_list.container",
        "film_list.poster_container",
        "film_page.title",
        "film_page.year",
        "pagination.pagination_container",
        "attributes.data_film_id"
    ]
    
    all_found = True
    for selector_path in test_selectors:
        selector = get_selector(selector_path)
        if selector:
            print(f"   ✅ {selector_path}: {selector}")
        else:
            print(f"   ❌ {selector_path}: NOT FOUND")
            all_found = False
    
    if not all_found:
        print("❌ Some selectors were not found")
        return False
    
    # Test 3: Test category-specific access
    print("\nTest 3: Testing category-specific access...")
    
    film_list_selectors = selector_config.get_film_list_selectors()
    film_page_selectors = selector_config.get_film_page_selectors()
    pagination_selectors = selector_config.get_pagination_selectors()
    attributes = selector_config.get_attributes()
    
    print(f"   ✅ Film list selectors: {len(film_list_selectors)} items")
    print(f"   ✅ Film page selectors: {len(film_page_selectors)} items")
    print(f"   ✅ Pagination selectors: {len(pagination_selectors)} items")
    print(f"   ✅ Attributes: {len(attributes)} items")
    
    # Test 4: Validate JSON structure
    print("\nTest 4: Validating JSON structure...")
    required_categories = ['film_list', 'film_page', 'pagination', 'attributes']
    missing_categories = []
    
    for category in required_categories:
        if category not in selectors:
            missing_categories.append(category)
    
    if missing_categories:
        print(f"   ❌ Missing categories: {', '.join(missing_categories)}")
        return False
    else:
        print("   ✅ All required categories present")
    
    # Test 5: Validate key selectors exist
    print("\nTest 5: Validating key selectors exist...")
    key_selectors = {
        'film_list.data_item_slug': 'For modern film list extraction',
        'film_list.data_film_slug': 'For legacy film list extraction',
        'film_page.title': 'For film title extraction',
        'film_page.year': 'For film year extraction',
        'pagination.pagination_container': 'For pagination detection'
    }
    
    missing_key_selectors = []
    for selector_path, description in key_selectors.items():
        if not get_selector(selector_path):
            missing_key_selectors.append(f"{selector_path} ({description})")
    
    if missing_key_selectors:
        print("   ❌ Missing key selectors:")
        for missing in missing_key_selectors:
            print(f"      - {missing}")
        return False
    else:
        print("   ✅ All key selectors present")
    
    print("\n🎉 All selector validation tests passed!")
    print("   The framework can now dynamically use selectors from the configuration.")
    print("   Selectors can be modified in letterboxd_selectors.json without code changes.")
    
    return True


def show_current_config():
    """Display the current selector configuration."""
    print("\n📋 Current Selector Configuration:")
    print("=" * 50)
    
    selectors = get_selectors()
    
    def print_dict(d, indent=0):
        for key, value in d.items():
            if isinstance(value, dict):
                print("  " * indent + f"{key}:")
                print_dict(value, indent + 1)
            else:
                print("  " * indent + f"{key}: {value}")
    
    print_dict(selectors)


if __name__ == "__main__":
    success = main()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--show-config":
        show_current_config()
    
    sys.exit(0 if success else 1)
