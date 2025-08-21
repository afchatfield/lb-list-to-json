#!/usr/bin/env python3
"""
Letterboxd Scraping Framework - CLI Interface (Refactored)
Comprehensive command-line interface for web scraping Letterboxd with improved code structure.
"""

import click
import json
import logging
import os
import sys
import subprocess
from typing import Optional, List, Dict, Any
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# Add current directory to path for imports
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from scrapers.letterboxd_scraper import LetterboxdScraper
from core.base_session import BaseSession
from core.progress_utils import create_dual_progress_bars, create_parallel_progress_bars, create_progress_bar
from core.cli_helpers import CLIHelper, ScrapingMode, execute_scraping_by_mode
from parsers.letterboxd_parser import (
    LetterboxdParser, FilmDataFrameBuilder, 
    create_letterboxd_dataframe, create_summary_dataframe, create_genre_analysis_dataframe
)

# Set up logging - initially just to file, console will be configured based on flags
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
    ]
)

logger = logging.getLogger(__name__)


@click.group()
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.option('--verbose', is_flag=True, help='Show detailed logging output (default: quiet)')
@click.option('--log-file', default='scraping.log', help='Log file path')
@click.pass_context
def cli(ctx, debug, verbose, log_file):
    """
    🎬 Letterboxd Scraping Framework
    
    Hierarchical OOP framework for efficient Letterboxd scraping with JSON requests,
    Selenium capabilities, and comprehensive testing.
    """
    ctx.ensure_object(dict)
    
    # Configure logging based on flags
    root_logger = logging.getLogger()
    
    # Remove any existing console handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            root_logger.removeHandler(handler)
    
    # Set logging level
    if debug:
        root_logger.setLevel(logging.DEBUG)
        ctx.obj['debug'] = True
    else:
        root_logger.setLevel(logging.INFO)
        ctx.obj['debug'] = False
    
    # Add console handler only if verbose mode is enabled
    if verbose or debug:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        root_logger.addHandler(console_handler)
        ctx.obj['verbose'] = True
    else:
        ctx.obj['verbose'] = False
    
    ctx.obj['log_file'] = log_file
    
    # Welcome message
    click.echo("🎬 Letterboxd Scraping Framework v2.0")
    click.echo("=" * 50)
    
    if not verbose and not debug:
        click.echo("💡 Tip: Use --verbose to see detailed logging output")


@cli.group()
@click.pass_context
def scrape(ctx):
    """🕷️ Scraping commands for films, lists, and user data."""
    ctx.obj['scraper'] = LetterboxdScraper()


@scrape.command()
@click.argument('film_slug')
@click.option('--output', '-o', default=None, help='Output JSON file path')
@click.option('--pretty', is_flag=True, help='Pretty print JSON output')
@click.option('--verbose', is_flag=True, help='Show detailed logging for this command')
@click.pass_context
def film(ctx, film_slug, output, pretty, verbose):
    """
    Scrape detailed information for a specific film.
    
    FILM_SLUG: The Letterboxd film slug (e.g., 'parasite-2019')
    """
    scraper = ctx.obj['scraper']
    
    # Handle verbose logging for this command
    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
    
    try:
        click.echo(f"🎭 Scraping film: {film_slug}")
        
        with click.progressbar(length=1, label='Extracting film data') as bar:
            film_data = scraper.get_film_details(film_slug)
            bar.update(1)
        
        # Determine output
        if output:
            output_path = output
        else:
            output_path = f"film_details_{film_slug}.json"
        
        # Save data
        with open(output_path, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(film_data, f, indent=2, ensure_ascii=False)
            else:
                json.dump(film_data, f, ensure_ascii=False)
        
        click.echo(f"✅ Film data saved to: {output_path}")
        click.echo(f"📊 Extracted {len(film_data)} fields")
        
        # Show key info
        click.echo(f"\n🎬 {film_data.get('title', 'N/A')} ({film_data.get('year', 'N/A')})")
        click.echo(f"🎭 Director: {film_data.get('director', 'N/A')}")
        click.echo(f"🌍 Countries: {', '.join(film_data.get('countries', []))}")
        
    except Exception as e:
        click.echo(f"❌ Error scraping film: {e}", err=True)
        sys.exit(1)
    finally:
        CLIHelper.cleanup_temp_verbose_logging(temp_verbose)


@scrape.command()
@click.argument('username', required=False)
@click.argument('list_slug', required=False)
@click.option('--output-dir', '-d', default='output', help='Output directory for results')
@click.option('--format', '-f', type=click.Choice(['json', 'csv', 'both']), default='both', help='Output format')
@click.option('--filename', help='Custom filename (without extension)')
@click.option('--parallel', is_flag=True, help='Use parallel processing for faster scraping')
@click.option('--workers', type=int, help='Number of parallel workers (default: auto-detect)')
@click.option('--predefined', help='Use predefined list key instead of username/list_slug')
@click.option('--verbose', is_flag=True, help='Show detailed logging for this command')
@click.pass_context
def list_basic(ctx, username, list_slug, output_dir, format, filename, parallel, workers, predefined, verbose):
    """
    Scrape all basic film info from a Letterboxd list (all pages).
    
    USERNAME: Letterboxd username (optional if --predefined is used)
    LIST_SLUG: List slug/name (optional if --predefined is used)
    
    Use --predefined flag with a predefined list key to scrape popular lists.
    Example: python main.py scrape list-basic --predefined my_top_100
    OR: python main.py scrape list-basic username list_name
    """
    scraper = ctx.obj['scraper']
    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
    
    try:
        # Validate arguments
        if predefined:
            if username or list_slug:
                click.echo("⚠️  Warning: USERNAME and LIST_SLUG arguments are ignored when using --predefined")
            list_info = CLIHelper.validate_predefined_list(predefined, scraper.PREDEFINED_LISTS)
            if not list_info:
                return
            username, list_slug = list_info
            list_identifier = predefined
            click.echo(f"📝 Scraping predefined list: {predefined} ({username}/{list_slug})")
        else:
            if not username or not list_slug:
                click.echo("❌ ERROR: USERNAME and LIST_SLUG are required when not using --predefined")
                click.echo("💡 Use --predefined with a predefined list key, or provide both USERNAME and LIST_SLUG")
                return
            list_identifier = f"{username}/{list_slug}"
            click.echo(f"📝 Scraping basic info from list: {username}/{list_slug}")
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Get pagination info
        pagination_info = scraper.get_list_pagination_info(username, list_slug)
        total_pages = pagination_info['total_pages']
        estimated_films = pagination_info['total_films_estimate']
        
        click.echo(f"📊 Found {total_pages} pages with ~{estimated_films} films")
        
        # Show processing mode
        CLIHelper.show_processing_info(parallel, workers)
        
        # Execute scraping
        films = execute_scraping_by_mode(scraper, ScrapingMode.BASIC, username, list_slug, 
                                       parallel, workers, total_pages)
        
        click.echo(f"✅ Extracted {len(films)} films from {total_pages} pages")
        
        # Generate filename and save
        base_filename = CLIHelper.generate_filename(
            predefined or f"{username}_{list_slug}", ScrapingMode.BASIC, filename
        )
        CLIHelper.save_films_data(films, output_path, base_filename, format)
        
        # Show summary
        CLIHelper.show_summary(list_identifier, films, total_pages, output_path)
        
    except Exception as e:
        click.echo(f"❌ Error scraping list: {e}", err=True)
        sys.exit(1)
    finally:
        CLIHelper.cleanup_temp_verbose_logging(temp_verbose)


@scrape.command()
@click.argument('username', required=False)
@click.argument('list_slug', required=False)
@click.option('--output-dir', '-d', default='output', help='Output directory for results')
@click.option('--format', '-f', type=click.Choice(['json', 'csv', 'both']), default='both', help='Output format')
@click.option('--filename', help='Custom filename (without extension)')
@click.option('--continue-on-error', is_flag=True, help='Continue if individual film details fail')
@click.option('--parallel', is_flag=True, help='Use parallel processing for faster scraping')
@click.option('--workers', type=int, help='Number of parallel workers (default: auto-detect)')
@click.option('--predefined', help='Use predefined list key instead of username/list_slug')
@click.option('--verbose', is_flag=True, help='Show detailed logging for this command')
@click.pass_context
def list_detailed(ctx, username, list_slug, output_dir, format, filename, continue_on_error, parallel, workers, predefined, verbose):
    """
    Scrape detailed film info from a Letterboxd list (visits each film page).
    
    USERNAME: Letterboxd username (optional if --predefined is used)
    LIST_SLUG: List slug/name (optional if --predefined is used)
    
    Use --predefined flag with a predefined list key to scrape popular lists.
    Example: python main.py scrape list-detailed --predefined letterboxd_250
    OR: python main.py scrape list-detailed username list_name
    """
    scraper = ctx.obj['scraper']
    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
    
    try:
        # Validate arguments
        if predefined:
            if username or list_slug:
                click.echo("⚠️  Warning: USERNAME and LIST_SLUG arguments are ignored when using --predefined")
            list_info = CLIHelper.validate_predefined_list(predefined, scraper.PREDEFINED_LISTS)
            if not list_info:
                return
            username, list_slug = list_info
            list_identifier = predefined
            click.echo(f"🎭 Scraping detailed predefined list: {predefined} ({username}/{list_slug})")
        else:
            if not username or not list_slug:
                click.echo("❌ ERROR: USERNAME and LIST_SLUG are required when not using --predefined")
                click.echo("💡 Use --predefined with a predefined list key, or provide both USERNAME and LIST_SLUG")
                return
            list_identifier = f"{username}/{list_slug}"
            click.echo(f"🎭 Scraping detailed info from list: {username}/{list_slug}")
        
        click.echo("⚠️  This will visit each film page and may take some time...")
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Get pagination info
        pagination_info = scraper.get_list_pagination_info(username, list_slug)
        total_pages = pagination_info['total_pages']
        estimated_films = pagination_info['total_films_estimate']
        
        click.echo(f"📊 Found {total_pages} pages with ~{estimated_films} films")
        
        # Show processing mode
        CLIHelper.show_processing_info(parallel, workers)
        
        # Use the optimized two-phase scraping approach
        click.echo("🚀 Using optimized two-phase scraping:")
        click.echo("   Phase 1: Fast collection of basic film info")
        click.echo("   Phase 2: Parallel processing of detailed info")
        
        # Execute scraping
        detailed_films = execute_scraping_by_mode(scraper, ScrapingMode.DETAILED, username, list_slug, 
                                                parallel, workers, total_pages)
        
        click.echo(f"✅ Processed {len(detailed_films)} films with detailed information")
        
        # Generate filename and save
        base_filename = CLIHelper.generate_filename(
            predefined or f"{username}_{list_slug}", ScrapingMode.DETAILED, filename
        )
        CLIHelper.save_films_data(detailed_films, output_path, base_filename, format)
        
        # Show summary
        CLIHelper.show_summary(list_identifier, detailed_films, total_pages, output_path)
        
    except Exception as e:
        click.echo(f"❌ Error scraping detailed list: {e}", err=True)
        sys.exit(1)
    finally:
        CLIHelper.cleanup_temp_verbose_logging(temp_verbose)


@scrape.command()
@click.argument('username', required=False)
@click.argument('list_slug', required=False)
@click.option('--output-dir', '-d', default='output', help='Output directory for results')
@click.option('--format', '-f', type=click.Choice(['json', 'csv', 'both']), default='both', help='Output format')
@click.option('--filename', help='Custom filename (without extension)')
@click.option('--parallel', is_flag=True, help='Use parallel processing for faster scraping')
@click.option('--workers', type=int, help='Number of parallel workers (default: auto-detect)')
@click.option('--predefined', help='Use predefined list key instead of username/list_slug')
@click.option('--verbose', is_flag=True, help='Show detailed logging for this command')
@click.pass_context
def list_ratings(ctx, username, list_slug, output_dir, format, filename, parallel, workers, predefined, verbose):
    """
    Scrape only ratings and stats data from a Letterboxd list (fast).
    
    USERNAME: Letterboxd username (optional if --predefined is used)
    LIST_SLUG: List slug/name (optional if --predefined is used)
    
    Use --predefined flag with a predefined list key to scrape popular lists.
    Example: python main.py scrape list-ratings --predefined letterboxd_250
    OR: python main.py scrape list-ratings username list_name
    """
    scraper = ctx.obj['scraper']
    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
    
    try:
        # Validate arguments
        if predefined:
            if username or list_slug:
                click.echo("⚠️  Warning: USERNAME and LIST_SLUG arguments are ignored when using --predefined")
            list_info = CLIHelper.validate_predefined_list(predefined, scraper.PREDEFINED_LISTS)
            if not list_info:
                return
            username, list_slug = list_info
            list_identifier = predefined
            click.echo(f"📊 Scraping ratings/stats from predefined list: {predefined} ({username}/{list_slug})")
        else:
            if not username or not list_slug:
                click.echo("❌ ERROR: USERNAME and LIST_SLUG are required when not using --predefined")
                click.echo("💡 Use --predefined with a predefined list key, or provide both USERNAME and LIST_SLUG")
                return
            list_identifier = f"{username}/{list_slug}"
            click.echo(f"📊 Scraping ratings and stats from list: {username}/{list_slug}")
        
        click.echo("🚀 Fast mode: Only collecting ratings and statistics data")
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Get pagination info
        pagination_info = scraper.get_list_pagination_info(username, list_slug)
        total_pages = pagination_info['total_pages']
        estimated_films = pagination_info['total_films_estimate']
        
        click.echo(f"📊 Found {total_pages} pages with ~{estimated_films} films")
        
        # Show processing mode
        CLIHelper.show_processing_info(parallel, workers)
        
        # Execute scraping
        films_with_stats = execute_scraping_by_mode(scraper, ScrapingMode.RATINGS_STATS, username, list_slug, 
                                                  parallel, workers, total_pages)
        
        click.echo(f"✅ Processed {len(films_with_stats)} films with ratings and stats")
        
        # Generate filename and save
        base_filename = CLIHelper.generate_filename(
            predefined or f"{username}_{list_slug}", ScrapingMode.RATINGS_STATS, filename
        )
        CLIHelper.save_films_data(films_with_stats, output_path, base_filename, format)
        
        # Show summary
        CLIHelper.show_summary(list_identifier, films_with_stats, total_pages, output_path)
        
    except Exception as e:
        click.echo(f"❌ Error scraping ratings/stats: {e}", err=True)
        sys.exit(1)
    finally:
        CLIHelper.cleanup_temp_verbose_logging(temp_verbose)


@cli.group()
def test():
    """🧪 Testing commands for framework validation."""
    pass


@test.command()
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.option('--markers', '-m', help='Run tests with specific markers (e.g., "not slow")')
@click.option('--coverage', is_flag=True, help='Generate coverage report')
@click.option('--html-report', is_flag=True, help='Generate HTML test report')
def all(verbose, markers, coverage, html_report):
    """Run all tests in the framework."""
    try:
        click.echo("🧪 Running all framework tests...")
        
        # Build pytest command
        cmd = ["python", "-m", "pytest", "tests/"]
        
        if verbose:
            cmd.append("-v")
        
        if markers:
            cmd.extend(["-m", markers])
        
        if coverage:
            cmd.extend(["--cov=scrapers", "--cov=core", "--cov-report=term-missing"])
        
        if html_report:
            cmd.extend(["--html=test_report.html", "--self-contained-html"])
        
        # Add standard options
        cmd.extend(["--tb=short", "--durations=10"])
        
        click.echo(f"Running: {' '.join(cmd)}")
        
        # Run tests
        result = subprocess.run(cmd, cwd=current_dir)
        
        if result.returncode == 0:
            click.echo("✅ All tests passed!")
            if html_report:
                click.echo("📊 HTML report generated: test_report.html")
        else:
            click.echo("❌ Some tests failed!")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Test execution failed: {e}", err=True)
        sys.exit(1)


@test.command()
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def connection(verbose):
    """Test basic connection and scraping functionality."""
    try:
        click.echo("🔗 Testing basic connections...")
        
        cmd = ["python", "-m", "pytest", "tests/test_letterboxd.py"]
        if verbose:
            cmd.append("-v")
        
        result = subprocess.run(cmd, cwd=current_dir)
        
        if result.returncode == 0:
            click.echo("✅ Connection tests passed!")
        else:
            click.echo("❌ Connection tests failed!")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Connection test failed: {e}", err=True)
        sys.exit(1)


@test.command()
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.option('--quick', is_flag=True, help='Run quick tests only (skip slow ones)')
def extraction(verbose, quick):
    """Test film data extraction functionality."""
    try:
        click.echo("🎭 Testing film extraction...")
        
        cmd = ["python", "-m", "pytest", "tests/test_film_extraction.py"]
        if verbose:
            cmd.append("-v")
        if quick:
            cmd.extend(["-m", "not slow"])
        
        result = subprocess.run(cmd, cwd=current_dir)
        
        if result.returncode == 0:
            click.echo("✅ Extraction tests passed!")
        else:
            click.echo("❌ Extraction tests failed!")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Extraction test failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--output', '-o', default='framework_info.json', help='Output file for info')
def info(output):
    """Display framework information and capabilities."""
    try:
        scraper = LetterboxdScraper()
        
        info_data = {
            "framework": {
                "name": "Letterboxd Scraping Framework",
                "version": "2.0.0",
                "components": ["scrapers", "parsers", "core", "tests"]
            },
            "capabilities": {
                "scraping": [
                    "Film details extraction",
                    "List films extraction", 
                    "Combined data processing",
                    "JSON configuration-based selectors",
                    "Rate limiting and error handling",
                    "Predefined list support with flags"
                ],
                "testing": [
                    "Pytest framework integration",
                    "Connection validation",
                    "Data extraction validation",
                    "Error handling tests",
                    "Integration workflow tests"
                ]
            },
            "predefined_lists": list(scraper.PREDEFINED_LISTS.keys()),
            "available_commands": {
                "scrape": ["film", "list-basic", "list-detailed", "list-ratings"],
                "test": ["all", "connection", "extraction"],
                "data": ["to-dataframe", "summary", "genres", "combine"]
            }
        }
        
        # Display info
        click.echo("ℹ️  Framework Information:")
        click.echo("=" * 40)
        click.echo(f"Name: {info_data['framework']['name']}")
        click.echo(f"Version: {info_data['framework']['version']}")
        click.echo(f"Components: {', '.join(info_data['framework']['components'])}")
        
        click.echo(f"\n📋 Predefined Lists:")
        for list_key in info_data['predefined_lists']:
            username, list_slug = scraper.PREDEFINED_LISTS[list_key]
            click.echo(f"  - {list_key}: {username}/{list_slug}")
        
        click.echo(f"\n🛠️  Available Commands:")
        for group, commands in info_data['available_commands'].items():
            click.echo(f"  {group}: {', '.join(commands)}")
        
        click.echo(f"\n📖 Usage Examples:")
        click.echo("  # Scrape basic info from a custom list:")
        click.echo("  python main.py scrape list-basic username list-name")
        click.echo("  # Scrape detailed info from a predefined list:")
        click.echo("  python main.py scrape list-detailed user list --predefined letterboxd_250")
        click.echo("  # Scrape ratings only from predefined list:")
        click.echo("  python main.py scrape list-ratings user list --predefined my_top_100")
        
        # Save detailed info
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(info_data, f, indent=2, ensure_ascii=False)
        
        click.echo(f"\n💾 Detailed info saved to: {output}")
        
    except Exception as e:
        click.echo(f"❌ Error getting framework info: {e}", err=True)
        sys.exit(1)


@cli.command()
def logs():
    """View recent framework logs."""
    try:
        log_file = 'scraping.log'
        
        if not os.path.exists(log_file):
            click.echo("📝 No log file found.")
            return
        
        click.echo("📖 Recent Log Entries:")
        click.echo("=" * 40)
        
        with open(log_file, 'r') as f:
            lines = f.readlines()
            
        # Show last 20 lines
        for line in lines[-20:]:
            click.echo(line.rstrip())
            
    except Exception as e:
        click.echo(f"❌ Error reading logs: {e}", err=True)


@cli.group()
def data():
    """📊 Data processing commands for DataFrame analysis."""
    pass


@data.command()
@click.argument('input_file')
@click.option('--output', '-o', default=None, help='Output CSV file path')
@click.option('--clean', is_flag=True, default=True, help='Apply data cleaning')
@click.option('--show-summary', is_flag=True, help='Display summary statistics')
def to_dataframe(input_file, output, clean, show_summary):
    """
    Convert JSON film data to pandas DataFrame.
    
    INPUT_FILE: Path to JSON file containing film data
    """
    try:
        click.echo(f"📊 Converting {input_file} to DataFrame...")
        
        # Load JSON data
        with open(input_file, 'r', encoding='utf-8') as f:
            films_data = json.load(f)
        
        if not isinstance(films_data, list):
            click.echo("❌ Input file must contain a list of films", err=True)
            return
        
        # Create DataFrame
        df = create_letterboxd_dataframe(films_data, clean_data=clean)
        
        click.echo(f"✅ Created DataFrame with {len(df)} films and {len(df.columns)} columns")
        
        # Show summary if requested
        if show_summary:
            click.echo("\n📈 DataFrame Summary:")
            click.echo("-" * 40)
            click.echo(f"Shape: {df.shape}")
            click.echo(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
            
            # Show column info
            click.echo(f"\nColumns ({len(df.columns)}):")
            for col in df.columns:
                non_null_count = df[col].count()
                data_type = str(df[col].dtype)
                click.echo(f"  {col}: {non_null_count}/{len(df)} non-null ({data_type})")
        
        # Save to CSV if output specified
        if output:
            df.to_csv(output, index=False, encoding='utf-8')
            click.echo(f"💾 DataFrame saved to: {output}")
        else:
            # Show first few rows
            click.echo(f"\n🔍 Preview (first 3 rows):")
            click.echo(df.head(3).to_string())
        
    except Exception as e:
        click.echo(f"❌ Error converting to DataFrame: {e}", err=True)
        sys.exit(1)


@data.command()
@click.argument('input_file')
@click.option('--output', '-o', default=None, help='Output CSV file path')
def summary(input_file, output):
    """
    Generate summary statistics from film data.
    
    INPUT_FILE: Path to JSON file containing film data
    """
    try:
        click.echo(f"📈 Generating summary for {input_file}...")
        
        # Load JSON data
        with open(input_file, 'r', encoding='utf-8') as f:
            films_data = json.load(f)
        
        # Create summary DataFrame
        summary_df = create_summary_dataframe(films_data)
        
        click.echo(f"✅ Generated summary with {len(summary_df)} metrics")
        
        # Display summary
        click.echo("\n📊 Summary Statistics:")
        click.echo("=" * 50)
        for _, row in summary_df.iterrows():
            click.echo(f"{row['metric']}: {row['value']}")
            click.echo(f"  {row['description']}")
            click.echo()
        
        # Save if output specified
        if output:
            summary_df.to_csv(output, index=False)
            click.echo(f"💾 Summary saved to: {output}")
        
    except Exception as e:
        click.echo(f"❌ Error generating summary: {e}", err=True)
        sys.exit(1)


@data.command()
@click.argument('input_file')
@click.option('--output', '-o', default=None, help='Output CSV file path')
@click.option('--min-count', default=1, help='Minimum count to include genre')
def genres(input_file, output, min_count):
    """
    Analyze genre distribution in film data.
    
    INPUT_FILE: Path to JSON file containing film data
    """
    try:
        click.echo(f"🎭 Analyzing genres in {input_file}...")
        
        # Load JSON data
        with open(input_file, 'r', encoding='utf-8') as f:
            films_data = json.load(f)
        
        # Create genre analysis DataFrame
        genre_df = create_genre_analysis_dataframe(films_data)
        
        if genre_df.empty:
            click.echo("❌ No genre data found in the input file")
            return
        
        # Filter by minimum count
        genre_df = genre_df[genre_df['count'] >= min_count]
        
        click.echo(f"✅ Found {len(genre_df)} genres (min count: {min_count})")
        
        # Display top genres
        click.echo("\n🎭 Top Genres:")
        click.echo("-" * 40)
        for _, row in genre_df.head(10).iterrows():
            click.echo(f"{row['genre']}: {row['count']} films ({row['percentage']:.1f}%)")
        
        if len(genre_df) > 10:
            click.echo(f"   ... and {len(genre_df) - 10} more genres")
        
        # Save if output specified
        if output:
            genre_df.to_csv(output, index=False)
            click.echo(f"\n💾 Genre analysis saved to: {output}")
        
    except Exception as e:
        click.echo(f"❌ Error analyzing genres: {e}", err=True)
        sys.exit(1)


@data.command()
@click.argument('input_files', nargs=-1, required=True)
@click.option('--output', '-o', default='combined_data.csv', help='Output CSV file path')
@click.option('--clean', is_flag=True, default=True, help='Apply data cleaning')
def combine(input_files, output, clean):
    """
    Combine multiple JSON film files into a single DataFrame.
    
    INPUT_FILES: Paths to JSON files containing film data
    """
    try:
        click.echo(f"🔗 Combining {len(input_files)} files...")
        
        all_films = []
        
        for file_path in input_files:
            click.echo(f"  Loading {file_path}...")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                films_data = json.load(f)
            
            if isinstance(films_data, list):
                all_films.extend(films_data)
            else:
                click.echo(f"⚠️  Skipping {file_path} (not a list)")
        
        if not all_films:
            click.echo("❌ No valid film data found in input files")
            return
        
        # Create combined DataFrame
        df = create_letterboxd_dataframe(all_films, clean_data=clean)
        
        click.echo(f"✅ Combined {len(df)} films from {len(input_files)} files")
        
        # Remove duplicates based on film_slug if available
        if 'film_slug' in df.columns:
            initial_count = len(df)
            df = df.drop_duplicates(subset=['film_slug']).reset_index(drop=True)
            duplicates_removed = initial_count - len(df)
            if duplicates_removed > 0:
                click.echo(f"🧹 Removed {duplicates_removed} duplicate films")
        
        # Save combined data
        df.to_csv(output, index=False, encoding='utf-8')
        click.echo(f"💾 Combined DataFrame saved to: {output}")
        
        # Show summary
        click.echo(f"\n📊 Final dataset: {df.shape[0]} films, {df.shape[1]} columns")
        
    except Exception as e:
        click.echo(f"❌ Error combining files: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
