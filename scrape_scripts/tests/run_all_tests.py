#!/usr/bin/env python3
"""
Test runner for all Letterboxd scraper tests.
Runs both basic connection tests and advanced film extraction tests.
"""

import sys
import os
import unittest
import logging

# Add the scrape_scripts directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_results.log'),
        logging.StreamHandler()
    ]
)


def discover_and_run_tests():
    """Discover and run all tests in the tests directory."""
    
    print("🧪 Letterboxd Scraper Test Suite")
    print("=" * 50)
    
    # Discover all test files
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(os.path.abspath(__file__))
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        descriptions=True,
        failfast=False
    )
    
    print(f"Running tests from: {start_dir}")
    print("-" * 50)
    
    result = runner.run(suite)
    
    print("-" * 50)
    print("📊 Test Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.failures:
        print("\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n💥 ERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    if result.wasSuccessful():
        print("\n🎉 All tests passed!")
        return True
    else:
        print(f"\n❌ {len(result.failures + result.errors)} test(s) failed")
        return False


def run_specific_test_class(test_class_name):
    """Run a specific test class."""
    
    print(f"🧪 Running specific test class: {test_class_name}")
    print("=" * 50)
    
    # Import test modules
    if test_class_name.startswith('TestLetterboxd'):
        from test_letterboxd import TestLetterboxdScraper
        suite = unittest.TestLoader().loadTestsFromTestCase(TestLetterboxdScraper)
    elif test_class_name.startswith('TestFilm'):
        from test_film_extraction import TestFilmExtraction, TestFilmExtractionIntegration
        if 'Integration' in test_class_name:
            suite = unittest.TestLoader().loadTestsFromTestCase(TestFilmExtractionIntegration)
        else:
            suite = unittest.TestLoader().loadTestsFromTestCase(TestFilmExtraction)
    else:
        print(f"❌ Unknown test class: {test_class_name}")
        return False
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


def main():
    """Main test runner function."""
    
    if len(sys.argv) > 1:
        # Run specific test class
        test_class = sys.argv[1]
        success = run_specific_test_class(test_class)
    else:
        # Run all tests
        success = discover_and_run_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
