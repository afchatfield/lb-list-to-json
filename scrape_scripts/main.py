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
import csv
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
from list_creators import ListCreator, ListConfig, SortBy

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
    
    Main Commands:
    - scrape: Extract data from Letterboxd lists and films
    - create: Generate new lists from existing data
    - data: Analyze and process film data
    - batch-lists: Create multiple lists automatically using predefined configurations
    - info: Display framework information
    - logs: View recent framework logs
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
def create():
    """📂 Commands to create new lists from existing data."""
    pass


@create.command(name='list-from-files')
@click.option('--file', '-f', 'json_files', required=True, multiple=True, 
              help='Path to a JSON data file. Use this option multiple times to provide multiple files (e.g., film lists, stats dictionaries).')
@click.option('--output-file', '-o', required=True, help='Path to save the generated list (without extension for both formats).')
@click.option('--format', type=click.Choice(['json', 'csv', 'both']), default='both', 
              help='Output format: json (structured), csv (films only), or both.')
@click.option('--title', help='Title for the list.')
@click.option('--description', default="", help='Description for the list.')
@click.option('--limit', type=int, default=None, help='Number of films to include in the list. No limit by default.')
@click.option('--sort-by', type=click.Choice([e.value for e in SortBy]), default=SortBy.AVERAGE_RATING.value,
              help='Field to sort the list by.')
@click.option('--sort-ascending', is_flag=True, help='Sort in ascending order instead of descending.')
@click.option('--countries', help='Comma-separated list of countries to filter by (e.g., "Japan,South Korea").')
@click.option('--languages', help='Comma-separated list of languages to filter by (e.g., "Japanese,Korean").')
@click.option('--include-secondary-languages', is_flag=True, help='Include secondary languages in the language filter.')
@click.option('--genres', help='Comma-separated list of genres to filter by.')
@click.option('--min-year', type=int, help='Minimum release year to include.')
@click.option('--max-year', type=int, help='Maximum release year to include.')
@click.option('--min-runtime', type=int, help='Minimum runtime in minutes.')
@click.option('--max-runtime', type=int, help='Maximum runtime in minutes.')
@click.option('--min-rating', type=float, help='Minimum average rating.')
@click.option('--max-rating', type=float, help='Maximum average rating.')
@click.option('--cutoff', type=click.Choice(['ratings', 'watches']), help='Apply minimum cutoff based on ratings count or watches count.')
@click.option('--cutoff-limit', type=int, help='Minimum number of ratings/watches required (required when --cutoff is used).')
@click.option('--simple-json', is_flag=True, default=False, help='Output a simple JSON array of film IDs and names.')
def list_from_files(json_files, output_file, format, title, description, limit, sort_by, sort_ascending,
                    countries, languages, include_secondary_languages, genres,
                    min_year, max_year, min_runtime, max_runtime, min_rating, max_rating, cutoff, cutoff_limit, simple_json):
    """
    Generate a custom film list from existing JSON data files.

    This command merges data from multiple JSON files, filters and sorts them based
    on the provided criteria, and saves the result as a new list.

    Example:
    
    python scrape_scripts/main.py create list-from-files \\
      --file "path/to/film_details.json" \\
      --file "path/to/film_ratings.json" \\
      --output-file "output/my_custom_list" \\
      --format "both" \\
      --title "My Awesome Film List" \\
      --limit 100 \\
      --sort-by "average_rating" \\
      --countries "USA,UK" \\
      --cutoff "ratings" \\
      --cutoff-limit 2000
    """
    try:
        click.echo(f"🛠️  Creating list '{title}'...")
        
        # Validate cutoff parameters
        if cutoff and cutoff_limit is None:
            click.echo("❌ Error: --cutoff-limit is required when --cutoff is specified", err=True)
            sys.exit(1)
        if cutoff_limit is not None and not cutoff:
            click.echo("❌ Error: --cutoff must be specified when --cutoff-limit is provided", err=True)
            sys.exit(1)
        
        # 1. Initialize ListCreator with the provided files
        list_creator = ListCreator(json_files=list(json_files))
        
        # 2. Parse comma-separated filter strings into lists
        countries_list = countries.split(',') if countries else None
        languages_list = languages.split(',') if languages else None
        genres_list = genres.split(',') if genres else None
        
        # 3. Define the configuration for the list with all filter parameters
        config = ListConfig(
            title=title,
            description=description,
            limit=limit,
            sort_by=SortBy(sort_by),
            sort_ascending=sort_ascending,
            countries=countries_list,
            languages=languages_list,
            include_secondary_languages=include_secondary_languages,
            genres=genres_list,
            min_year=min_year,
            max_year=max_year,
            min_runtime=min_runtime,
            max_runtime=max_runtime,
            min_rating=min_rating,
            max_rating=max_rating,
            cutoff_type=cutoff,
            cutoff_limit=cutoff_limit
        )
        
        # 4. Generate the list using the provided criteria
        generated_list = list_creator.create_list(
            config=config,
            output_path=Path(output_file).with_suffix('.json'),
            output_format='json',
            simple_json=simple_json
        )
        
        # 5. Extract just the films list for CSV output
        if isinstance(generated_list, dict):
            films_list = generated_list['films']
            total_found = generated_list['total_found']
            films_returned = generated_list['films_returned']
        else:
            films_list = generated_list
            total_found = None
            films_returned = len(films_list)
        
        # 6. Save files based on format preference
        output_path = Path(output_file)
        files_created = []
        
        def save_csv(file_path, films_data):
            """Save films data as CSV"""
            if not films_data:
                return
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = films_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(films_data)
        
        if format in ['json', 'both']:
            json_file = output_path.with_suffix('.json')
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(generated_list, f, indent=2, ensure_ascii=False)
            files_created.append(str(json_file))
        
        if format in ['csv', 'both']:
            csv_file = output_path.with_suffix('.csv')
            save_csv(csv_file, films_list)
            files_created.append(str(csv_file))
        
        if simple_json:
            simple_json_file = output_path.with_suffix('.simple.json')
            simple_data = [{"id": int(film.get("film_id") or film.get("id", 0)), "name": film.get("name")} for film in films_list]
            with open(simple_json_file, 'w', encoding='utf-8') as f:
                json.dump(simple_data, f, indent=2, ensure_ascii=False)
            files_created.append(str(simple_json_file))
        
        # 7. Show completion message
        click.echo(f"✅ List saved to: {', '.join(files_created)}")
        
        # Only show summary if we have metadata or it's relevant
        if total_found is not None:
            click.echo(f"📊 Found {total_found} films, returned {films_returned}")
        else:
            click.echo(f"📊 Generated list with {films_returned} films")

    except ValueError as e:
        click.echo(f"❌ Configuration Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ An unexpected error occurred: {e}", err=True)
        logger.exception("An unexpected error occurred in create-list command")
        sys.exit(1)


@cli.command(name='batch-lists')
@click.option('--config', default='configs/batch_configurations.json', help='Path to batch configuration file.')
@click.option('--input-files', '-i', 'input_files_paths', multiple=True, help='Paths to input JSON data files. If not provided, finds newest files in output folder.')
@click.option('--output-dir', default='output/batch_lists', help='Output directory for the generated lists.')
@click.option('--simple-json/--no-simple-json', default=True, help='Also generate a simple JSON array of films with id and name, in order (default: enabled).')
@click.pass_context
def batch_lists(ctx, config, input_files_paths, output_dir, simple_json):
    """
    Create multiple lists from a batch configuration file.
    
    Iterates over a JSON configuration file, running the list creation 
    process for each item.
    """
    click.echo("🚀 Starting batch list creation...")
    
    # Ensure output directory exists
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # 1. Determine input files
    if input_files_paths:
        files_to_use = list(input_files_paths)
        click.echo(f"ℹ️ Using provided input files: {files_to_use}")
    else:
        try:
            output_folder = Path('output')
            
            def find_latest_file(pattern):
                files = list(output_folder.glob(pattern))
                if not files: return None
                return max(files, key=lambda f: f.stat().st_mtime)

            detailed_file = find_latest_file('all_the_films_detailed*.json')
            ratings_file = find_latest_file('all_the_films_ratings_stats*.json')
            
            files_to_use = []
            if detailed_file: files_to_use.append(str(detailed_file))
            if ratings_file: files_to_use.append(str(ratings_file))
            
            if not files_to_use:
                click.echo("❌ Error: No input files provided and couldn't find default files in 'output/' folder.", err=True)
                sys.exit(1)
            click.echo(f"ℹ️ Found latest input files: {files_to_use}")
        except Exception as e:
            click.echo(f"❌ Error finding input files: {e}", err=True)
            sys.exit(1)

    # 2. Load batch configurations
    try:
        with open(config, 'r', encoding='utf-8') as f:
            batch_config = json.load(f)
        configurations = batch_config.get('configurations', [])
        if not configurations:
            click.echo("⚠️ No configurations found in the file.", err=True)
            return
    except FileNotFoundError:
        click.echo(f"❌ Error: Configuration file not found at '{config}'", err=True)
        sys.exit(1)
    except json.JSONDecodeError:
        click.echo(f"❌ Error: Could not decode JSON from '{config}'", err=True)
        sys.exit(1)

    # 3. Process each configuration
    failed_lists = []
    successful_lists = 0
    
    with tqdm(total=len(configurations), desc="Processing lists") as pbar:
        for i, list_config in enumerate(configurations):
            list_name = list_config.get('name', f"list_{i+1}")
            pbar.set_description(f"Processing '{list_name}'")
            
            try:
                # Prepare arguments for list_from_files
                args = {
                    'json_files': files_to_use,
                    'output_file': str(output_path / list_config.get('output_filename', list_name)),
                    'format': 'both',
                    'title': list_config.get('title'),
                    'description': list_config.get('description', ""),
                    'limit': list_config.get('limit'),
                    'sort_by': list_config.get('sort_by', 'average_rating'),
                    'sort_ascending': list_config.get('sort_ascending', False),
                    'countries': ','.join(list_config['countries']) if 'countries' in list_config else None,
                    'languages': ','.join(list_config['languages']) if 'languages' in list_config else None,
                    'include_secondary_languages': list_config.get('include_secondary_languages', False),
                    'genres': ','.join(list_config['genres']) if 'genres' in list_config else None,
                    'min_year': list_config.get('min_year'),
                    'max_year': list_config.get('max_year'),
                    'min_runtime': list_config.get('min_runtime'),
                    'max_runtime': list_config.get('max_runtime'),
                    'min_rating': list_config.get('min_rating'),
                    'max_rating': list_config.get('max_rating'),
                    'cutoff': list_config.get('cutoff'),
                    'cutoff_limit': list_config.get('cutoff_limit'),
                    'simple_json': simple_json
                }
                
                # Remove None values so defaults in list_from_files are used
                args = {k: v for k, v in args.items() if v is not None}

                # Use invoke to call the command
                ctx.invoke(list_from_files, **args)
                successful_lists += 1

            except Exception as e:
                error_msg = f"Failed to create list '{list_name}': {e}"
                click.echo(f"\n❌ {error_msg}", err=True)
                
                # Try to identify the problematic argument
                problem_arg = "Unknown"
                if isinstance(e, (KeyError, TypeError)):
                    problem_arg = f"Likely a missing or malformed parameter in config for '{list_name}'"
                
                failed_lists.append({'name': list_name, 'reason': str(e), 'problem_arg': problem_arg})
            
            pbar.update(1)

    # 4. Final summary
    click.echo("\n" + "="*50)
    click.echo("✅ Batch processing complete.")
    click.echo(f"  - Successfully created: {successful_lists} lists")
    click.echo(f"  - Failed: {len(failed_lists)} lists")

    if failed_lists:
        click.echo("\n❌ Failed Lists Summary:")
        for failed in failed_lists:
            click.echo(f"  - Name: {failed['name']}")
            click.echo(f"    Reason: {failed['reason']}")
            click.echo(f"    Possible Cause: {failed['problem_arg']}")


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


@data.command()
@click.argument('input_files', nargs=-1, required=True)
@click.option('--country', help='Filter by country (e.g. france)')
@click.option('--language', help='Filter by language (e.g. french)')
@click.option('--top', default=100, help='Number of films to return (default: 100)')
@click.option('--sort-by', type=click.Choice(['average_rating', 'release_year', 'runtime', 'name']), 
              default='average_rating', help='Field to sort by (default: average_rating)')
@click.option('--output', '-o', default=None, help='Output JSON file path')
@click.option('--show-stats', is_flag=True, help='Show dataset statistics')
def create_list(input_files, country, language, top, sort_by, output, show_stats):
    """
    Create a custom list of top films from specific countries and/or languages.
    
    INPUT_FILES: Paths to JSON files containing film data
    
    Examples:
    python main.py data create-list *.json --country france --top 250 --language french
    python main.py data create-list file.json --country usa --sort-by release_year
    """
    try:
        click.echo(f"🎬 Creating filtered list from {len(input_files)} files...")
        
        # Convert sort_by string to enum
        sort_enum = SortBy(sort_by)
        
        # Create the list
        creator = ListCreator(input_files)
        
        # Show statistics if requested
        if show_stats:
            stats = creator.get_statistics()
            click.echo("\n📊 Dataset Statistics:")
            click.echo("=" * 40)
            click.echo(f"Total films: {stats['total_films']}")
            click.echo(f"Films with ratings: {stats['films_with_ratings']}")
            if stats['year_range']['min']:
                click.echo(f"Year range: {stats['year_range']['min']}-{stats['year_range']['max']}")
            if stats['rating_range']['average']:
                click.echo(f"Average rating: {stats['rating_range']['average']:.2f}")
            click.echo(f"Countries available: {stats['unique_countries']}")
            click.echo(f"Languages available: {stats['unique_languages']}")
            
            # Show available countries and languages if no filters specified
            if not country and not language:
                available_countries = creator.get_available_countries()[:20]  # Show first 20
                click.echo(f"\n🌍 Available countries (first 20): {', '.join(available_countries)}")
                available_languages = creator.get_available_languages()[:20]  # Show first 20
                click.echo(f"🗣️  Available languages (first 20): {', '.join(available_languages)}")
                click.echo("\nUse --country and/or --language to filter")
                return
        
        # Create the list
        countries = [country] if country else None
        languages = [language] if language else None
        
        result = creator.create_country_language_list(
            limit=top,
            countries=countries,
            languages=languages,
            sort_by=sort_enum
        )
        
        # Display results
        click.echo(f"\n🎯 {result['title']}")
        click.echo(f"📝 {result['description']}")
        click.echo(f"📊 Found {result['total_found']} matching films, returning top {result['films_returned']}")
        
        # Show top films
        click.echo(f"\n🏆 Top Films:")
        click.echo("-" * 60)
        for i, film in enumerate(result['films'][:10], 1):
            rating = f"⭐ {film.get('average_rating', 'N/A')}" if film.get('average_rating') else ""
            year = f"({film.get('release_year', 'N/A')})" if film.get('release_year') else ""
            countries_str = f"[{', '.join(film.get('countries', [])[:2])}]" if film.get('countries') else ""
            click.echo(f"{i:2}. {film['name']} {year} {countries_str} {rating}")
        
        if len(result['films']) > 10:
            click.echo(f"    ... and {len(result['films']) - 10} more films")
        
        # Save if output specified
        if output:
            with open(output, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            click.echo(f"\n💾 List saved to: {output}")
        
    except Exception as e:
        click.echo(f"❌ Error creating list: {e}", err=True)
        sys.exit(1)









@data.command()
@click.argument('input_files', nargs=-1, required=True)
def dataset_info(input_files):
    """
    Show detailed information about the film dataset.
    
    INPUT_FILES: Paths to JSON files containing film data
    """
    try:
        click.echo(f"📊 Analyzing dataset from {len(input_files)} files...")
        
        creator = ListCreator(input_files)
        stats = creator.get_statistics()
        
        click.echo("\n🎬 Dataset Overview:")
        click.echo("=" * 50)
        click.echo(f"Total films: {stats['total_films']:,}")
        click.echo(f"Films with ratings: {stats['films_with_ratings']:,}")
        click.echo(f"Films with years: {stats['films_with_years']:,}")
        click.echo(f"Films with runtime: {stats['films_with_runtime']:,}")
        
        if stats['year_range']['min']:
            click.echo(f"\n📅 Year Range:")
            click.echo(f"Earliest film: {stats['year_range']['min']}")
            click.echo(f"Latest film: {stats['year_range']['max']}")
            click.echo(f"Span: {stats['year_range']['max'] - stats['year_range']['min']} years")
        
        if stats['rating_range']['average']:
            click.echo(f"\n⭐ Ratings:")
            click.echo(f"Lowest rating: {stats['rating_range']['min']:.1f}")
            click.echo(f"Highest rating: {stats['rating_range']['max']:.1f}")
            click.echo(f"Average rating: {stats['rating_range']['average']:.2f}")
        
        if stats['runtime_range']['average']:
            click.echo(f"\n⏱️  Runtime:")
            click.echo(f"Shortest film: {stats['runtime_range']['min']} minutes")
            click.echo(f"Longest film: {stats['runtime_range']['max']} minutes")
            click.echo(f"Average runtime: {stats['runtime_range']['average']:.1f} minutes")
        
        click.echo(f"\n🌍 Geographic Data:")
        click.echo(f"Unique countries: {stats['unique_countries']}")
        click.echo(f"Unique languages: {stats['unique_languages']}")
        
        click.echo(f"\n🎭 Content Data:")
        click.echo(f"Unique genres: {stats['unique_genres']}")
        
        # Show top countries, languages, and genres
        countries = creator.get_available_countries()[:10]
        languages = creator.get_available_languages()[:10]
        genres = creator.get_available_genres()[:10]
        
        if countries:
            click.echo(f"\n🌍 Top Countries: {', '.join(countries)}")
        if languages:
            click.echo(f"🗣️  Top Languages: {', '.join(languages)}")
        if genres:
            click.echo(f"🎭 Top Genres: {', '.join(genres)}")
        
        click.echo(f"\n💡 Use other commands to create filtered lists from this data!")
        
    except Exception as e:
        click.echo(f"❌ Error analyzing dataset: {e}", err=True)
        sys.exit(1)


@data.command()
@click.argument('csv_file')
@click.option('--output', '-o', default=None, help='Output JSON file path (default: same name as CSV with .simple.json extension)')
def csv_to_simple_json(csv_file, output):
    """
    Convert a CSV file from batch lists to simple JSON format.
    
    Creates a simple JSON array with film id and name in the same order as the CSV.
    Format: [{"id": 74748, "name": "Film name"}, ...]
    
    CSV_FILE: Path to the CSV file to convert
    
    Example:
    python main.py data csv-to-simple-json output/batch_lists/top_100_korean_films.csv
    """
    try:
        csv_path = Path(csv_file)
        
        if not csv_path.exists():
            click.echo(f"❌ Error: CSV file not found: {csv_file}", err=True)
            sys.exit(1)
        
        # Determine output path
        if output:
            output_path = Path(output)
        else:
            output_path = csv_path.with_suffix('.simple.json')
        
        click.echo(f"🔄 Converting {csv_path.name} to simple JSON...")
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Check if required columns exist
        if 'film_id' not in df.columns or 'name' not in df.columns:
            click.echo(f"❌ Error: CSV must contain 'film_id' and 'name' columns", err=True)
            click.echo(f"Available columns: {', '.join(df.columns.tolist())}", err=True)
            sys.exit(1)
        
        # Create simple JSON data in the same order as CSV
        simple_data = []
        for _, row in df.iterrows():
            film_data = {
                "id": int(row['film_id']),
                "name": str(row['name'])
            }
            simple_data.append(film_data)
        
        # Save JSON file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(simple_data, f, indent=2, ensure_ascii=False)
        
        click.echo(f"✅ Converted {len(simple_data)} films to simple JSON")
        click.echo(f"💾 Saved to: {output_path}")
        
        # Show sample of first few entries
        if simple_data:
            click.echo(f"\n🔍 Sample entries:")
            for i, film in enumerate(simple_data[:3]):
                click.echo(f"  {i+1}. {film['name']} (ID: {film['id']})")
            if len(simple_data) > 3:
                click.echo(f"  ... and {len(simple_data) - 3} more films")
        
    except Exception as e:
        click.echo(f"❌ Error converting CSV to simple JSON: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
