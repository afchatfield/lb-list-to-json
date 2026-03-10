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


@scrape.command(name='browse-films')
@click.option('--produced-in', '-p', default=None, help='Country where films were produced (e.g. france, italy, japan, usa). Optional.')
@click.option('--rated-in', '-r', default=None, help='Country whose rating preferences to use for weighting (e.g. france, italy, japan, usa). Optional.')
@click.option('--language', '-l', default=None, help='Language slug for the URL filter (e.g. french, italian, japanese). Optional.')
@click.option('--filter-language', '-L', multiple=True,
              help='Post-scrape language filter using DB data. Accepts multiple values. '
                   'Matches against primary_language and other_languages from all_the_films_detailed. '
                   'E.g. -L Turkish -L Kurdish -L English')
@click.option('--primary-language-only', is_flag=True,
              help='When using -L, only match against primary_language (ignore other_languages).')
@click.option('--limit', '-n', type=int, default=250, help='Max number of films to collect (default: 250)')
@click.option('--sort', '-s', type=click.Choice(['rating', 'popular', 'name']), default='rating', help='Sort order (default: rating)')
@click.option('--delay', type=float, default=2.5, help='Delay between page requests in seconds (default: 2.5)')
@click.option('--output-dir', '-d', default='output', help='Output directory for results')
@click.option('--format', '-f', type=click.Choice(['json', 'csv', 'both']), default='both', help='Output format')
@click.option('--filename', help='Custom filename (without extension)')
@click.option('--filter-db', 'filter_db_path', default=None, 
              help='Path to all_the_films JSON to filter against. Auto-detects latest if not provided.')
@click.option('--no-filter', is_flag=True, help='Skip filtering against all_the_films database')
@click.option('--verbose', is_flag=True, help='Show detailed logging for this command')
@click.pass_context
def browse_films(ctx, produced_in, rated_in, language, filter_language, primary_language_only, limit, sort, delay, output_dir, format, filename, filter_db_path, no_filter, verbose):
    """
    Scrape films from Letterboxd browse pages by country and language.
    
    Uses the AJAX endpoint to extract films in the order Letterboxd displays them.
    The page order reflects Letterboxd's weighted ranking algorithm (not just raw
    average rating). Each film gets a browse_rank preserving its original position.
    
    Films are filtered against the all_the_films database to only include known films.
    
    You can specify:
    - --produced-in: Filter films produced in a specific country
    - --rated-in: Use rating preferences from a specific country (weighted ranking)
    - --filter-language / -L: Post-scrape language filter (multiple allowed, uses DB data)
    - Both together: e.g. films produced in USA rated highly in France
    
    Examples:
    
    \b
      # Films produced in France, rated by French users
      python main.py scrape browse-films --produced-in france --rated-in france --language french
      
      # Films produced in Italy, rated by Italian users  
      python main.py scrape browse-films -p italy -r italy -l italian --limit 100
      
      # Films produced in USA, but weighted by Japanese preferences
      python main.py scrape browse-films --produced-in usa --rated-in japan --limit 500
      
      # Turkey rated by Turkish users, filter to Turkish/Kurdish/English language films
      python main.py scrape browse-films -p turkey -r turkey -L Turkish -L Kurdish -L English
      
      # Only production country filter (no rating weighting)
      python main.py scrape browse-films --produced-in japan --language japanese --no-filter
    """
    scraper = ctx.obj['scraper']
    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
    
    # Validate: at least one country must be specified
    if not produced_in and not rated_in:
        click.echo("❌ Error: Must specify at least one of --produced-in or --rated-in")
        return
    
    if filter_language and no_filter:
        click.echo("❌ Error: --filter-language requires the database filter (remove --no-filter)")
        return
    
    try:
        # Build label for display
        label_parts = []
        if produced_in:
            label_parts.append(f"produced in {produced_in}")
        if rated_in:
            label_parts.append(f"rated in {rated_in}")
        if language:
            label_parts.append(f"url language: {language}")
        if filter_language:
            label_parts.append(f"filter languages: {', '.join(filter_language)}")
        label = ", ".join(label_parts)
        click.echo(f"🌍 Scraping browse films: {label} (sorted by {sort})")
        click.echo(f"📊 Target: up to {limit} films | Delay: {delay}s between pages")
        
        # Create output directory under rating_in_country/
        output_path = Path(output_dir) / 'rating_in_country'
        output_path.mkdir(parents=True, exist_ok=True)
        
        # ---- Load filter database ----
        known_slugs = None
        db_films_by_slug = {}
        
        if not no_filter:
            db_path = filter_db_path
            if not db_path:
                # Auto-detect latest all_the_films file — always search the default 'output'
                # directory regardless of --output-dir, so a custom output path doesn't break
                # DB detection.
                import glob
                # Always include the canonical output dir; also check the specified dir in
                # case it genuinely contains its own DB files.
                search_dirs = []
                default_output = Path('output')
                if Path(output_dir) != default_output:
                    search_dirs.append(default_output)
                search_dirs.append(Path(output_dir))

                if filter_language:
                    # --filter-language needs the detailed DB which has language data
                    glob_patterns = ['all_the_films_detailed_*.json']
                else:
                    glob_patterns = [
                        'all_the_films_detailed_*.json',
                        'all_the_films_ratings_stats_*.json',
                    ]

                candidates = []
                for search_dir in search_dirs:
                    for pattern in glob_patterns:
                        candidates.extend(glob.glob(str(search_dir / pattern)))

                if candidates:
                    db_path = max(candidates, key=lambda p: Path(p).stat().st_mtime)
                elif filter_language:
                    click.echo("❌ Error: --filter-language requires all_the_films_detailed DB (has language data)")
                    click.echo("   💡 Run: python main.py scrape list-detailed --predefined all_the_films")
                    return
            
            if db_path and Path(db_path).exists():
                click.echo(f"📁 Loading filter database: {Path(db_path).name}")
                with open(db_path, 'r', encoding='utf-8') as f:
                    db_films = json.load(f)
                known_slugs = {f.get('film_slug') for f in db_films if f.get('film_slug')}
                db_films_by_slug = {f['film_slug']: f for f in db_films if f.get('film_slug')}
                click.echo(f"   ✅ {len(known_slugs):,} known films loaded")
                
                # Pre-filter known_slugs by language if --filter-language is set
                if filter_language:
                    filter_langs_lower = {lang.lower() for lang in filter_language}
                    pre_filter_count = len(known_slugs)
                    lang_matched_slugs = set()
                    for slug in known_slugs:
                        db_entry = db_films_by_slug.get(slug, {})
                        primary = (db_entry.get('primary_language') or '').lower()
                        if primary in filter_langs_lower:
                            lang_matched_slugs.add(slug)
                        elif not primary_language_only:
                            others = [l.lower() for l in (db_entry.get('other_languages') or [])]
                            if set(others) & filter_langs_lower:
                                lang_matched_slugs.add(slug)
                    known_slugs = lang_matched_slugs
                    mode_label = 'primary only' if primary_language_only else 'primary + spoken'
                    click.echo(f"   🗣️  Language filter ({', '.join(filter_language)}) [{mode_label}]: {len(known_slugs):,} of {pre_filter_count:,} films match")
            else:
                click.echo("⚠️  No all_the_films database found — skipping filter (use --no-filter to suppress)")
                click.echo("   💡 Run: python main.py scrape list-basic --predefined all_the_films")
        
        # ---- Scrape browse pages ----
        with tqdm(desc="Scraping pages", unit="page") as pbar:
            def progress_cb(page, total_pages, matched_count, total_count):
                if total_pages is not None:
                    pbar.total = total_pages
                pbar.n = page
                if known_slugs is not None:
                    pbar.set_postfix(matched=matched_count, scraped=total_count)
                else:
                    pbar.set_postfix(films=total_count)
                pbar.refresh()
            
            raw_films = scraper.scrape_browse_films(
                production_country=produced_in,
                rating_country=rated_in,
                language=language,
                sort=sort,
                limit=limit,
                delay=delay,
                progress_callback=progress_cb,
                known_slugs=known_slugs,
            )
        
        click.echo(f"\n🎬 Scraped {len(raw_films)} films from browse pages")
        
        # ---- Filter against database ----
        if known_slugs:
            included = []
            excluded = []
            
            for film in raw_films:
                slug = film.get('film_slug', '')
                if slug in known_slugs:
                    # Enrich with data from the database
                    db_entry = db_films_by_slug.get(slug, {})
                    enriched = {**film}
                    # Add useful fields from DB if present
                    for key in ('genres', 'countries', 'primary_language', 'other_languages', 'director', 'runtime',
                                'total_ratings', 'watches_count', 'fans_count'):
                        if key in db_entry and db_entry[key]:
                            enriched[key] = db_entry[key]
                    included.append(enriched)
                else:
                    excluded.append(film)
            
            click.echo(f"🔍 Filter results:")
            click.echo(f"   ✅ {len(included)} films matched in database")
            click.echo(f"   ❌ {len(excluded)} films excluded (not in all_the_films)")
            
            # Show excluded films for verification
            if excluded:
                show_count = min(len(excluded), 20)
                click.echo(f"\n📋 Excluded films (showing {show_count}/{len(excluded)}):")
                for i, film in enumerate(excluded[:show_count], 1):
                    rank = film.get('browse_rank', '?')
                    rating_str = f"⭐ {film['average_rating']}" if film.get('average_rating') else ""
                    click.echo(f"   #{rank:<4} {film.get('name_with_year', film.get('name', '?')):50s} {rating_str}")
                if len(excluded) > show_count:
                    click.echo(f"   ... and {len(excluded) - show_count} more")
            
            films = included
        else:
            films = raw_films
        
        # Apply limit after all filters
        films = films[:limit]
        
        if not films:
            click.echo("❌ No films found!")
            return
        
        click.echo(f"\n✅ Final result: {len(films)} films")
        
        # Show top 10
        click.echo(f"\n🏆 Top {min(10, len(films))} films (rank = position on Letterboxd page):")
        click.echo("-" * 70)
        for film in films[:10]:
            rank = film.get('browse_rank', '?')
            name = film.get('name_with_year', film.get('name', '?'))
            rating = f"⭐ {film['average_rating']}" if film.get('average_rating') else ""
            director = f"({film['director']})" if film.get('director') else ""
            click.echo(f"  #{rank:<4} {name:45s} {rating} {director}")
        
        # ---- Save ----
        # Build filename based on what was specified
        filename_parts = []
        if produced_in:
            filename_parts.append(f"produced_{produced_in}")
        if rated_in:
            filename_parts.append(f"rated_{rated_in}")
        if language:
            filename_parts.append(language)
        if filter_language:
            filename_parts.append("lang_" + "_".join(l.lower() for l in filter_language))
        
        default_filename = "browse_" + "_".join(filename_parts) + f"_by_{sort}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        base_filename = filename or default_filename
        CLIHelper.save_films_data(films, output_path, base_filename, format)
        
        click.echo(f"\n📊 Summary: {len(films)} films from {label}, sorted by {sort}")
        click.echo(f"   Output: {output_path.absolute()}")
        
    except Exception as e:
        click.echo(f"❌ Error scraping browse films: {e}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        CLIHelper.cleanup_temp_verbose_logging(temp_verbose)


@scrape.command(name='aggregate-region')
@click.argument('directory', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option('--min-appearances', '-m', type=int, default=1,
              help='Exclude films appearing in fewer than N source files (default: 1 = include all).')
@click.option('--limit', '-n', type=int, default=None,
              help='Cap the final output to the top N films (default: all).')
@click.option('--ratings-db', default=None,
              help='Path to all_the_films_ratings_stats JSON. Auto-detects latest in output/ if omitted.')
@click.option('--output-dir', '-d', default='output',
              help='Root output directory (default: output). Results go into <output-dir>/aggregate_region/.')
@click.option('--format', '-f', 'fmt', type=click.Choice(['json', 'csv', 'both']), default='both',
              help='Output format (default: both).')
@click.option('--filename', help='Custom output filename stem (without extension).')
@click.option('--average', 'simple_average', is_flag=True,
              help='Simple mode: average dense ranks across ALL lists in the directory, '
                   'then multiply by global average_rating. No per-country grouping or z-scoring. '
                   'Useful for flat directories like spanish-speaking lists.')
@click.option('--verbose', is_flag=True, help='Show detailed logging.')
@click.pass_context
def aggregate_region(ctx, directory, min_appearances, limit, ratings_db, output_dir, fmt, filename, simple_average, verbose):
    """
    Aggregate browse-films CSVs from a directory into a single ranked list.

    DIRECTORY: Folder containing *.csv files produced by 'scrape browse-films'.
    Each CSV represents one (production_country, rating_country) combination.
    Films absent from a given rating list receive a penalty rank of len(list) * 1.25.

    Algorithm:
    \b
      1. Parse filenames to discover production countries and their rating lists.
         e.g. produced_sweden_rated_denmark → production country "sweden",
              rating list "denmark".
      2. For each production country, collect all films across its rating lists.
         Average each film's dense rank across those lists (missing → penalty).
      3. Z-score normalise the averaged ranks WITHIN each production country
         (so Sweden films compete with Sweden films, not Danish ones).
      4. Multiply z-score × global average_rating (from all_the_films_ratings_stats).
      5. Merge all production-country DataFrames. If the same film (name+year+director)
         appears in multiple countries (co-productions), average the final_scores.
      6. Sort descending by final_score.

    Example:

    \b
      python main.py scrape aggregate-region output/scandinavia_ratings/ --limit 100
    """
    import glob
    import math
    import re
    import numpy as np
    from collections import defaultdict

    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))

    try:
        src_dir = Path(directory)
        csv_files = sorted(src_dir.glob('*.csv'))

        if not csv_files:
            click.echo(f"❌ No CSV files found in {src_dir}")
            return

        click.echo(f"📂 Directory : {src_dir.absolute()}")
        click.echo(f"📄 CSV files : {len(csv_files)}")

        # ==================================================================
        # Helpers
        # ==================================================================
        def _canon(name, year, director):
            return (
                str(name).strip().lower(),
                int(year) if year and str(year).isdigit() else 0,
                str(director).strip().lower(),
            )

        def _parse_list_field(val) -> str:
            """'[\"Sweden\", \"France\"]' → 'Sweden, France'"""
            import ast
            if val is None or (isinstance(val, float) and math.isnan(val)):
                return ''
            s = str(val).strip()
            if not s or s in ('nan', '[]'):
                return ''
            try:
                parsed = ast.literal_eval(s)
                if isinstance(parsed, list):
                    return ', '.join(str(x).strip(" '\"") for x in parsed if str(x).strip(" '\""))
            except Exception:
                pass
            s = s.strip('[]').replace("'", '').replace('"', '')
            return ', '.join(x.strip() for x in s.split(',') if x.strip())

        def _parse_filename(stem: str):
            """Return (produced_country, rated_country) or None."""
            m = re.search(r'produced_(.+?)_rated_(.+?)(?:_lang_|_by_)', stem)
            if m:
                return m.group(1), m.group(2)
            return None

        # ==================================================================
        # SIMPLE AVERAGE MODE  (--average flag)
        # ==================================================================
        if simple_average:
            click.echo("📐 Mode: simple average rank across all lists")

            # Load every CSV, dense-rank each, collect per-film ranks
            # film_registry[canon_key] = metadata
            film_registry: Dict[tuple, Dict[str, Any]] = {}
            # all_ranks[canon_key] = {stem: dense_rank}
            all_ranks: Dict[tuple, Dict[str, int]] = defaultdict(dict)
            all_stems_simple = []

            for csv_path in csv_files:
                stem = re.sub(r'_\d{8}_\d{6}$', '', csv_path.stem)  # trim timestamp
                try:
                    df = pd.read_csv(csv_path)
                except Exception as e:
                    click.echo(f"  ⚠️  Could not read {csv_path.name}: {e}")
                    continue

                required = {'film_slug', 'browse_rank', 'name', 'year'}
                if not required.issubset(df.columns):
                    click.echo(f"  ⚠️  Skipping {csv_path.name}: missing {required - set(df.columns)}")
                    continue

                if 'director' not in df.columns:
                    df['director'] = ''
                df = df.dropna(subset=['film_slug', 'browse_rank', 'name'])
                df = df.sort_values('browse_rank').reset_index(drop=True)

                for dense_rank, (_, row) in enumerate(df.iterrows(), start=1):
                    key = _canon(row['name'], row.get('year', 0), row.get('director', ''))
                    all_ranks[key][stem] = dense_rank
                    if key not in film_registry:
                        film_registry[key] = {
                            'film_slug': str(row['film_slug']),
                            'name': str(row['name']),
                            'year': int(row['year']) if str(row.get('year', '')).isdigit() else row.get('year', ''),
                            'director': str(row.get('director', '') or ''),
                            'browse_avg_rating': row.get('average_rating', None),
                            'countries': _parse_list_field(row.get('countries', '')),
                            'genres': _parse_list_field(row.get('genres', '')),
                            'primary_language': str(row.get('primary_language', '') or ''),
                        }

                all_stems_simple.append(stem)
                click.echo(f"  ✅ {csv_path.name}: {len(df)} films")

            n_lists = len(all_stems_simple)
            click.echo(f"\n🎬 Unique films: {len(film_registry)} | Lists: {n_lists}")

            if not film_registry:
                click.echo("❌ No films loaded.")
                return

            # Compute per-stem list lengths (= max dense rank seen in that list)
            stem_lengths: Dict[str, int] = {}
            for key2, ranks2 in all_ranks.items():
                for s, r in ranks2.items():
                    if s not in stem_lengths or r > stem_lengths[s]:
                        stem_lengths[s] = r

            # Compute average rank (missing → penalty = list_len * 1.25)
            rows_simple = []
            for key, meta in film_registry.items():
                appearances = len(all_ranks[key])
                if appearances < min_appearances:
                    continue
                rank_vals_final = []
                for stem in all_stems_simple:
                    if stem in all_ranks[key]:
                        rank_vals_final.append(float(all_ranks[key][stem]))
                    else:
                        rank_vals_final.append(stem_lengths.get(stem, 80) * 1.25)

                avg_rank = float(np.mean(rank_vals_final))
                rows_simple.append({
                    **meta,
                    'avg_rank': round(avg_rank, 2),
                    'appearances': appearances,
                    'n_rating_lists': n_lists,
                    'source_ranks': json.dumps(all_ranks[key]),
                    'global_avg_rating': None,
                    'rating_source': None,
                    'final_score': None,
                })

            # Load ratings DB
            ratings_by_slug: Dict[str, float] = {}
            db_path = ratings_db
            if not db_path:
                candidates = sorted(
                    glob.glob(str(Path('output') / 'all_the_films_ratings_stats_*.json')),
                    key=lambda p: Path(p).stat().st_mtime, reverse=True,
                )
                if candidates:
                    db_path = candidates[0]
            if db_path and Path(db_path).exists():
                click.echo(f"\n📁 Ratings DB: {Path(db_path).name}")
                with open(db_path, 'r', encoding='utf-8') as f:
                    db_films = json.load(f)
                for entry in db_films:
                    slug = entry.get('film_slug')
                    rating = entry.get('average_rating')
                    if slug and rating:
                        try:
                            ratings_by_slug[slug] = float(rating)
                        except (ValueError, TypeError):
                            pass
                click.echo(f"   ✅ {len(ratings_by_slug):,} ratings loaded")
            else:
                click.echo("⚠️  No ratings DB found — using browse-page ratings")

            # final_score = (1 / avg_rank) * global_avg_rating
            # (lower avg_rank = better, so invert it)
            for r in rows_simple:
                slug = r['film_slug']
                if slug in ratings_by_slug:
                    r['global_avg_rating'] = ratings_by_slug[slug]
                    r['rating_source'] = 'db'
                elif r.get('browse_avg_rating') is not None:
                    try:
                        r['global_avg_rating'] = float(r['browse_avg_rating'])
                        r['rating_source'] = 'browse'
                    except (ValueError, TypeError):
                        r['global_avg_rating'] = None
                        r['rating_source'] = 'missing'
                else:
                    r['global_avg_rating'] = None
                    r['rating_source'] = 'missing'

                if r['global_avg_rating'] is not None:
                    r['final_score'] = round((1.0 / r['avg_rank']) * r['global_avg_rating'], 6)
                else:
                    r['final_score'] = None

            simple_df = pd.DataFrame(rows_simple)
            simple_df = simple_df.sort_values('final_score', ascending=False, na_position='last').reset_index(drop=True)
            if limit:
                simple_df = simple_df.head(limit)
            simple_df.insert(0, 'rank', range(1, len(simple_df) + 1))
            simple_df['url'] = 'https://letterboxd.com/film/' + simple_df['film_slug'].astype(str) + '/'

            col_order_simple = [
                'rank', 'name', 'year', 'director', 'countries', 'genres', 'primary_language',
                'final_score', 'avg_rank', 'global_avg_rating', 'rating_source',
                'appearances', 'n_rating_lists', 'film_slug', 'url', 'browse_avg_rating', 'source_ranks',
            ]
            col_order_simple = [c for c in col_order_simple if c in simple_df.columns]
            simple_df = simple_df[col_order_simple]

            click.echo(f"\n✅ Final list: {len(simple_df)} films")
            click.echo(f"\n🏆 Top {min(15, len(simple_df))} films:")
            click.echo("-" * 80)
            for _, r in simple_df.head(15).iterrows():
                rating_str = f"⭐ {r['global_avg_rating']:.2f}" if r['global_avg_rating'] else "  n/a "
                click.echo(
                    f"  #{int(r['rank']):<4} {str(r['name'])[:38]:38s} {str(r.get('year',''))[:4]:5s} "
                    f"{rating_str}  avg_rank={r['avg_rank']:.1f}"
                )

            out_path = Path(output_dir) / 'aggregate_region'
            out_path.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = filename or f"aggregate_avg_{src_dir.name}_{ts}"

            if fmt in ('json', 'both'):
                json_file = out_path / f"{base_filename}.json"
                with open(json_file, 'w', encoding='utf-8') as fh:
                    json.dump(simple_df.to_dict(orient='records'), fh, indent=2, ensure_ascii=False)
                click.echo(f"💾 JSON saved: {json_file.name}")
            if fmt in ('csv', 'both'):
                csv_file = out_path / f"{base_filename}.csv"
                simple_df.to_csv(csv_file, index=False)
                click.echo(f"💾 CSV saved : {csv_file.name}")
            click.echo(f"   Output dir: {out_path.absolute()}")
            return

        # ==================================================================
        # 1. Group CSV files by production country
        # ==================================================================
        # groups[prod_country] = [(rated_country_label, csv_path), ...]
        groups: Dict[str, list] = defaultdict(list)

        for csv_path in csv_files:
            parsed = _parse_filename(csv_path.stem)
            if parsed:
                prod, rated = parsed
                groups[prod].append((rated, csv_path))
            else:
                click.echo(f"  ⚠️  Could not parse filename: {csv_path.name}")

        click.echo(f"\n🌍 Production countries detected: {', '.join(sorted(groups.keys()))}")
        for prod, members in sorted(groups.items()):
            rated_labels = [r for r, _ in members]
            click.echo(f"   {prod}: rated by {', '.join(rated_labels)} ({len(members)} lists)")

        # ==================================================================
        # 2. Load films per production country
        #    For each production country, read all its rating CSVs, dense-rank
        #    the films 1..N within each list, then average ranks across lists
        #    (missing films get penalty = list_len * 1.25).
        # ==================================================================
        # film_registry: canonical key → display metadata (first seen wins)
        film_registry: Dict[tuple, Dict[str, Any]] = {}

        # country_dfs will hold one DataFrame per production country
        country_dfs = []

        for prod_country, file_list in sorted(groups.items()):
            click.echo(f"\n📊 Processing: {prod_country} ({len(file_list)} rating lists)")

            # rating_lists[rated_label][canon_key] = dense_rank
            rating_lists: Dict[str, Dict[tuple, int]] = {}
            country_film_keys: set = set()

            for rated_label, csv_path in file_list:
                try:
                    df = pd.read_csv(csv_path)
                except Exception as e:
                    click.echo(f"  ⚠️  Could not read {csv_path.name}: {e}")
                    continue

                required = {'film_slug', 'browse_rank', 'name', 'year'}
                if not required.issubset(df.columns):
                    click.echo(f"  ⚠️  Skipping {csv_path.name}: missing {required - set(df.columns)}")
                    continue

                if 'director' not in df.columns:
                    df['director'] = ''
                df = df.dropna(subset=['film_slug', 'browse_rank', 'name'])
                df = df.sort_values('browse_rank').reset_index(drop=True)

                file_ranks: Dict[tuple, int] = {}
                for dense_rank, (_, row) in enumerate(df.iterrows(), start=1):
                    key = _canon(row['name'], row.get('year', 0), row.get('director', ''))
                    file_ranks[key] = dense_rank
                    country_film_keys.add(key)

                    if key not in film_registry:
                        film_registry[key] = {
                            'film_slug': str(row['film_slug']),
                            'name': str(row['name']),
                            'year': int(row['year']) if str(row.get('year', '')).isdigit() else row.get('year', ''),
                            'director': str(row.get('director', '') or ''),
                            'browse_avg_rating': row.get('average_rating', None),
                            'countries': _parse_list_field(row.get('countries', '')),
                            'genres': _parse_list_field(row.get('genres', '')),
                            'primary_language': str(row.get('primary_language', '') or ''),
                        }

                rating_lists[rated_label] = file_ranks
                click.echo(f"  ✅ rated by {rated_label}: {len(file_ranks)} films")

            if not rating_lists or not country_film_keys:
                click.echo(f"  ⚠️  No data for {prod_country}, skipping")
                continue

            # ----------------------------------------------------------
            # Average ranks across rating lists for this production country
            # ----------------------------------------------------------
            country_keys = sorted(country_film_keys)
            rated_labels = list(rating_lists.keys())

            avg_ranks = {}
            source_ranks_all = {}
            appearances_all = {}

            for key in country_keys:
                ranks_for_film = []
                sr = {}
                for rated_label in rated_labels:
                    file_ranks = rating_lists[rated_label]
                    list_len = len(file_ranks)
                    if key in file_ranks:
                        ranks_for_film.append(file_ranks[key])
                        sr[f"{prod_country}_x_{rated_label}"] = file_ranks[key]
                    else:
                        penalty = list_len * 1.25
                        ranks_for_film.append(penalty)

                avg_ranks[key] = np.mean(ranks_for_film)
                source_ranks_all[key] = sr
                appearances_all[key] = len(sr)

            # ----------------------------------------------------------
            # Z-score normalise the averaged ranks WITHIN this country
            # ----------------------------------------------------------
            keys_list = list(avg_ranks.keys())
            rank_vec = np.array([avg_ranks[k] for k in keys_list])
            mean_r = rank_vec.mean()
            std_r = rank_vec.std()

            if std_r == 0:
                z_vec = np.zeros(len(keys_list))
            else:
                z_vec = -(rank_vec - mean_r) / std_r   # negate: rank 1 → highest z

            # Build per-country results
            for i, key in enumerate(keys_list):
                meta = film_registry[key]
                country_dfs.append({
                    'canon_key': key,
                    'production_country': prod_country,
                    'film_slug': meta['film_slug'],
                    'name': meta['name'],
                    'year': meta['year'],
                    'director': meta['director'],
                    'countries': meta['countries'],
                    'genres': meta['genres'],
                    'primary_language': meta['primary_language'],
                    'browse_avg_rating': meta['browse_avg_rating'],
                    'avg_rank': round(float(avg_ranks[key]), 2),
                    'z_score': round(float(z_vec[i]), 6),
                    'appearances': appearances_all[key],
                    'n_rating_lists': len(rated_labels),
                    'source_ranks': json.dumps(source_ranks_all[key]),
                })

            click.echo(f"  📈 {len(keys_list)} films z-scored for {prod_country}")

        if not country_dfs:
            click.echo("❌ No data to aggregate.")
            return

        # ==================================================================
        # 3. Load global ratings DB
        # ==================================================================
        ratings_by_slug: Dict[str, float] = {}
        db_path = ratings_db

        if not db_path:
            candidates = sorted(
                glob.glob(str(Path('output') / 'all_the_films_ratings_stats_*.json')),
                key=lambda p: Path(p).stat().st_mtime,
                reverse=True,
            )
            if candidates:
                db_path = candidates[0]

        if db_path and Path(db_path).exists():
            click.echo(f"\n📁 Ratings DB: {Path(db_path).name}")
            with open(db_path, 'r', encoding='utf-8') as f:
                db_films = json.load(f)
            for entry in db_films:
                slug = entry.get('film_slug')
                rating = entry.get('average_rating')
                if slug and rating:
                    try:
                        ratings_by_slug[slug] = float(rating)
                    except (ValueError, TypeError):
                        pass
            click.echo(f"   ✅ {len(ratings_by_slug):,} ratings loaded")
        else:
            click.echo("⚠️  No ratings DB found — using browse-page ratings as fallback")

        # ==================================================================
        # 4. Multiply z_score × global_avg_rating → final_score
        # ==================================================================
        for row in country_dfs:
            slug = row['film_slug']
            if slug in ratings_by_slug:
                row['global_avg_rating'] = ratings_by_slug[slug]
                row['rating_source'] = 'db'
            else:
                br = row.get('browse_avg_rating')
                if br is not None:
                    try:
                        row['global_avg_rating'] = float(br)
                        row['rating_source'] = 'browse'
                    except (ValueError, TypeError):
                        row['global_avg_rating'] = None
                        row['rating_source'] = 'missing'
                else:
                    row['global_avg_rating'] = None
                    row['rating_source'] = 'missing'

            if row['global_avg_rating'] is not None:
                row['final_score'] = round(row['z_score'] * row['global_avg_rating'], 6)
            else:
                row['final_score'] = None

        # ==================================================================
        # 5. Merge into one DataFrame. If the same film (name+year+director)
        #    appears under multiple production countries, average the scores.
        # ==================================================================
        merged_df = pd.DataFrame(country_dfs)

        # For duplicate films (co-productions), aggregate:
        #   - average final_score, z_score, avg_rank, global_avg_rating
        #   - sum appearances
        #   - keep first occurrence's metadata
        #   - concatenate source_ranks dicts and production_country labels
        def _merge_source_ranks(sr_series):
            combined = {}
            for sr_json in sr_series:
                try:
                    combined.update(json.loads(sr_json))
                except Exception:
                    pass
            return json.dumps(combined)

        agg_funcs = {
            'film_slug': 'first',
            'name': 'first',
            'year': 'first',
            'director': 'first',
            'countries': 'first',
            'genres': 'first',
            'primary_language': 'first',
            'final_score': 'mean',
            'z_score': 'mean',
            'avg_rank': 'mean',
            'global_avg_rating': 'first',
            'rating_source': 'first',
            'browse_avg_rating': 'first',
            'appearances': 'sum',
            'n_rating_lists': 'sum',
            'production_country': lambda x: ', '.join(sorted(set(x))),
            'source_ranks': _merge_source_ranks,
        }

        merged_df['canon_key_str'] = merged_df['canon_key'].apply(str)
        merged_df = merged_df.groupby('canon_key_str', sort=False).agg(agg_funcs).reset_index(drop=True)

        # Round after averaging
        for col in ('final_score', 'z_score', 'avg_rank'):
            merged_df[col] = merged_df[col].round(6)

        # Apply min-appearances filter
        if min_appearances > 1:
            before = len(merged_df)
            merged_df = merged_df[merged_df['appearances'] >= min_appearances].reset_index(drop=True)
            click.echo(f"🔍 min-appearances={min_appearances}: {len(merged_df)} of {before} films kept")

        # Sort by final_score descending (nulls to bottom)
        merged_df = merged_df.sort_values('final_score', ascending=False, na_position='last').reset_index(drop=True)

        if limit:
            merged_df = merged_df.head(limit)

        merged_df.insert(0, 'rank', range(1, len(merged_df) + 1))

        # Build URL from slug
        merged_df['url'] = 'https://letterboxd.com/film/' + merged_df['film_slug'].astype(str) + '/'

        # Select and order columns
        col_order = [
            'rank', 'name', 'year', 'director', 'production_country',
            'countries', 'genres', 'primary_language',
            'final_score', 'z_score', 'avg_rank',
            'global_avg_rating', 'rating_source',
            'appearances', 'n_rating_lists',
            'film_slug', 'url', 'browse_avg_rating', 'source_ranks',
        ]
        col_order = [c for c in col_order if c in merged_df.columns]
        merged_df = merged_df[col_order]

        # ==================================================================
        # Preview
        # ==================================================================
        click.echo(f"\n✅ Final list: {len(merged_df)} films")
        click.echo(f"\n🏆 Top {min(15, len(merged_df))} films:")
        click.echo("-" * 85)
        for _, r in merged_df.head(15).iterrows():
            rating_str = f"⭐ {r['global_avg_rating']:.2f}" if r['global_avg_rating'] else "  n/a "
            z_str = f"z={r['z_score']:+.3f}"
            prod = r.get('production_country', '')
            click.echo(
                f"  #{int(r['rank']):<4} {str(r['name'])[:35]:35s} {str(r.get('year',''))[:4]:5s} "
                f"{rating_str}  {z_str}  [{prod}]"
            )

        # ==================================================================
        # Save
        # ==================================================================
        out_path = Path(output_dir) / 'aggregate_region'
        out_path.mkdir(parents=True, exist_ok=True)

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        default_stem = f"aggregate_{src_dir.name}_{ts}"
        base_filename = filename or default_stem

        if fmt in ('json', 'both'):
            json_file = out_path / f"{base_filename}.json"
            records = merged_df.to_dict(orient='records')
            with open(json_file, 'w', encoding='utf-8') as fh:
                json.dump(records, fh, indent=2, ensure_ascii=False)
            click.echo(f"💾 JSON saved: {json_file.name}")

        if fmt in ('csv', 'both'):
            csv_file = out_path / f"{base_filename}.csv"
            merged_df.to_csv(csv_file, index=False)
            click.echo(f"💾 CSV saved : {csv_file.name}")

        click.echo(f"   Output dir: {out_path.absolute()}")

    except Exception as e:
        click.echo(f"❌ Error in aggregate-region: {e}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        CLIHelper.cleanup_temp_verbose_logging(temp_verbose)


@scrape.command(name='fix-missing')
@click.option('--file', '-f', 'file_path', help='Path to ratings/stats JSON file (default: latest in output/)')
@click.option('--workers', type=int, default=1, help='Number of parallel workers for re-scraping (default: 1 for safety)')
@click.option('--output-dir', '-d', default='output', help='Output directory for updated files')
@click.option('--verbose', is_flag=True, help='Show detailed logging')
@click.pass_context
def fix_missing_data(ctx, file_path, workers, output_dir, verbose):
    """
    Fix missing ratings/stats data in an existing file.
    
    Scans a ratings/stats JSON file for films with missing average_rating,
    total_ratings, watches_count, lists_count, or likes_count, then re-scrapes
    only those films and updates the file (saves new JSON + CSV).
    
    Example: python main.py scrape fix-missing --file output/my_top_100_ratings_stats.json
    OR: python main.py scrape fix-missing  (uses latest file)
    """
    from pathlib import Path
    import json
    import glob
    from tqdm import tqdm
    
    scraper = ctx.obj['scraper']
    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
    
    try:
        output_path = Path(output_dir)
        
        # Find file to fix
        if file_path:
            json_file = Path(file_path)
            if not json_file.exists():
                click.echo(f"❌ ERROR: File not found: {file_path}")
                return
        else:
            # Find latest ratings_stats JSON file
            pattern = str(output_path / '*_ratings_stats_*.json')
            files = sorted(glob.glob(pattern), key=lambda x: Path(x).stat().st_mtime, reverse=True)
            if not files:
                click.echo(f"❌ ERROR: No ratings_stats JSON files found in {output_path}")
                return
            json_file = Path(files[0])
            click.echo(f"📁 Using latest file: {json_file.name}")
        
        # Load films
        with open(json_file, 'r', encoding='utf-8') as f:
            films = json.load(f)
        
        click.echo(f"📊 Loaded {len(films)} films from {json_file.name}")
        
        # Identify films with missing data (skip fans_count for <2000 watches)
        def needs_ratings_check(f):
            if not f.get('film_slug'):
                return False
            # Always check core ratings
            if not f.get('average_rating') or not f.get('total_ratings'):
                return True
            # Only check fans_count for films with 2000+ watches
            watches = f.get('watches_count', 0)
            if watches >= 2000 and not f.get('fans_count'):
                return True
            return False
        
        def needs_stats_check(f):
            if not f.get('film_slug'):
                return False
            return any(not f.get(field) for field in ('watches_count', 'lists_count', 'likes_count'))
        
        needs_ratings = [f for f in films if needs_ratings_check(f)]
        needs_stats = [f for f in films if needs_stats_check(f)]
        
        # Deduplicate (some might need both)
        needs_fix = {f['film_slug']: f for f in (needs_ratings + needs_stats)}
        
        if not needs_fix:
            click.echo("✅ All films have complete data — nothing to fix!")
            return
        
        click.echo(f"🔍 Found issues:")
        click.echo(f"   {len(needs_ratings)} films missing ratings data")
        click.echo(f"   {len(needs_stats)} films missing stats data")
        click.echo(f"   {len(needs_fix)} unique films need fixing")
        click.echo(f"   (Note: skipping fans_count for films with <2000 watches)")
        click.echo(f"\n🔄 Re-scraping with {workers} worker(s)...")

        
        # Re-scrape missing films
        fixed_data = {}
        
        if workers == 1:
            # Sequential with progress bar
            session = scraper.letterboxd_session
            with tqdm(total=len(needs_fix), desc="Re-scraping films", unit="film") as pbar:
                for slug, film in needs_fix.items():
                    pbar.set_postfix_str(slug)
                    result = scraper.get_film_ratings_and_stats(slug)
                    if result:
                        fixed_data[slug] = result
                    pbar.update(1)
        else:
            # Parallel
            basic_films = list(needs_fix.values())
            fixed_films = scraper.parallel_processor.get_ratings_stats_parallel(
                basic_films, max_workers=workers
            )
            for f in fixed_films:
                if f.get('film_slug'):
                    fixed_data[f['film_slug']] = f
        
        # Update original films list
        updated_count = 0
        for film in films:
            slug = film.get('film_slug')
            if slug in fixed_data:
                film.update(fixed_data[slug])
                updated_count += 1
        
        click.echo(f"✅ Updated {updated_count} films with new data")
        
        # Count remaining missing
        still_missing_r = sum(1 for f in films if f.get('film_slug') and 
                             any(not f.get(field) for field in RATINGS_FIELDS))
        still_missing_s = sum(1 for f in films if f.get('film_slug') and 
                             any(not f.get(field) for field in STATS_FIELDS))
        
        if still_missing_r or still_missing_s:
            click.echo(f"⚠️  Still missing: {still_missing_r} ratings, {still_missing_s} stats")
        else:
            click.echo("✨ All data now complete!")
        
        # Save updated files
        base_filename = json_file.stem + '_fixed'
        CLIHelper.save_films_data(films, output_path, base_filename, 'both')
        click.echo(f"\n💾 Updated files saved with '_fixed' suffix")
        
    except Exception as e:
        click.echo(f"❌ Error fixing missing data: {e}", err=True)
        import traceback
        traceback.print_exc()
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
        
        # Handle languages - can be either a string (comma-separated) or already a list
        if languages is None:
            languages_list = None
        elif isinstance(languages, list):
            languages_list = languages  # Already a list from batch processing
        else:
            languages_list = languages.split(',')  # String from CLI
        
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
                    'languages': list_config.get('languages'),  # Pass as list directly
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


@data.command()
@click.argument('reference_csv')
@click.option('--database-file', '-db', help='Path to comprehensive film database (JSON or CSV). If not provided, searches output/ for newest all_the_films file.')
@click.option('--output', '-o', default='higher_rated_by_country.csv', help='Output CSV file path')
@click.option('--min-rating-diff', type=float, default=0.01, help='Minimum rating difference to report (default: 0.01)')
@click.option('--show-all', is_flag=True, help='Show all films found, not just summary')
def find_higher_rated_by_country(reference_csv, database_file, output, min_rating_diff, show_all):
    """
    Find films with higher ratings than the current highest-rated film per country.
    
    REFERENCE_CSV: Path to Letterboxd export CSV with current highest-rated films per country.
                   Expected format: Position, Name, Year, URL, Description (where Description = country name)
    
    Example:
    python main.py data find-higher-rated-by-country reference_list.csv --database-file output/all_the_films_detailed.json
    """
    try:
        click.echo(f"🔍 Analyzing higher-rated films by country...")
        
        # Step 1: Parse reference CSV (Letterboxd export format)
        click.echo(f"📖 Reading reference list from: {reference_csv}")
        reference_films = _parse_letterboxd_export_csv(reference_csv)
        
        if not reference_films:
            click.echo("❌ No reference films found in CSV", err=True)
            sys.exit(1)
        
        click.echo(f"✅ Loaded {len(reference_films)} reference films")
        
        # Step 2: Load comprehensive film database
        if not database_file:
            click.echo("🔎 Searching for most recent all_the_films database file...")
            database_file = _find_latest_database_file()
            if not database_file:
                click.echo("❌ Could not find database file. Please specify with --database-file", err=True)
                sys.exit(1)
        
        click.echo(f"📊 Loading comprehensive database from: {database_file}")
        all_films = _load_film_database(database_file)
        click.echo(f"✅ Loaded {len(all_films)} films from database")
        
        # Step 3: Find higher-rated films for each country
        results = []
        countries_processed = 0
        countries_with_better_films = 0
        
        with click.progressbar(reference_films, label='Comparing by country') as films:
            for ref_film in films:
                country = ref_film['country']
                ref_name = ref_film['name']
                ref_rating = ref_film.get('rating')
                
                # Find ref film rating if not in CSV
                if ref_rating is None:
                    ref_rating = _find_film_rating(ref_name, ref_film.get('year'), all_films)
                
                if ref_rating is None:
                    click.echo(f"\n⚠️  Could not find rating for reference film: {ref_name} ({country})")
                    continue
                
                # Find all films from this country with higher rating
                better_films = _find_better_films_by_country(
                    country, ref_rating, all_films, min_rating_diff
                )
                
                countries_processed += 1
                
                if better_films:
                    countries_with_better_films += 1
                    for film in better_films:
                        results.append({
                            'country': country,
                            'reference_film': ref_name,
                            'reference_rating': ref_rating,
                            'better_film': film['name'],
                            'better_film_year': film.get('year', ''),
                            'better_film_rating': film.get('average_rating', film.get('rating', '')),
                            'rating_difference': round(film.get('average_rating', film.get('rating', 0)) - ref_rating, 2)
                        })
        
        # Step 4: Display and save results
        click.echo(f"\n📊 Analysis Results:")
        click.echo(f"   Countries processed: {countries_processed}")
        click.echo(f"   Countries with better films: {countries_with_better_films}")
        click.echo(f"   Total better films found: {len(results)}")
        
        if results:
            # Sort by rating difference (highest first)
            results.sort(key=lambda x: x['rating_difference'], reverse=True)
            
            # Save to CSV
            results_df = pd.DataFrame(results)
            results_df.to_csv(output, index=False, encoding='utf-8')
            click.echo(f"\n💾 Results saved to: {output}")
            
            # Display sample
            if show_all:
                click.echo(f"\n🎬 All films found ({len(results)}):")
                for r in results:
                    click.echo(f"\n{r['country']}:")
                    click.echo(f"  Reference: {r['reference_film']} (⭐ {r['reference_rating']})")
                    click.echo(f"  Better:    {r['better_film']} ({r['better_film_year']}) (⭐ {r['better_film_rating']}) [+{r['rating_difference']}]")
            else:
                click.echo(f"\n🎬 Top 10 biggest improvements:")
                for i, r in enumerate(results[:10], 1):
                    click.echo(f"\n{i}. {r['country']} (+{r['rating_difference']}):")
                    click.echo(f"   Reference: {r['reference_film']} (⭐ {r['reference_rating']})")
                    click.echo(f"   Better:    {r['better_film']} ({r['better_film_year']}) (⭐ {r['better_film_rating']})")
                
                if len(results) > 10:
                    click.echo(f"\n... and {len(results) - 10} more films. Use --show-all to see everything.")
        else:
            click.echo("\n✅ No higher-rated films found. Your list is up to date!")
        
    except Exception as e:
        click.echo(f"❌ Error finding higher-rated films: {e}", err=True)
        logger.exception("Error in find_higher_rated_by_country")
        sys.exit(1)


def _parse_letterboxd_export_csv(csv_path: str) -> List[Dict[str, Any]]:
    """Parse Letterboxd export CSV format."""
    films = []
    
    try:
        # Read the raw file to find where the actual data starts
        with open(csv_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()
        
        # Normalize line endings (Windows \r\n -> \n)
        raw_content = raw_content.replace('\r\n', '\n').replace('\r', '\n')
        
        lines = raw_content.split('\n')
        
        # Find the line that starts with "Position"
        data_start_line = None
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith('Position,Name,Year') or stripped.startswith('Position,'):
                data_start_line = i
                break
        
        if data_start_line is None:
            logger.error("Could not find data header row starting with 'Position'")
            logger.error(f"First 10 lines of file: {lines[:10]}")
            return films
        
        # Build a clean CSV string from the data section only
        data_lines = lines[data_start_line:]
        clean_csv = '\n'.join(data_lines)
        
        # Read CSV from the clean string
        from io import StringIO
        df = pd.read_csv(StringIO(clean_csv), encoding='utf-8')
        
        click.echo(f"   Found columns: {list(df.columns)}")
        click.echo(f"   Found {len(df)} rows")
        
        # Parse each film
        for _, row in df.iterrows():
            # Skip rows with missing essential data
            name = row.get('Name')
            description = row.get('Description')
            
            if pd.isna(name) or pd.isna(description):
                continue
            
            name_str = str(name).strip()
            desc_str = str(description).strip()
            
            # Skip header-like rows
            if name_str == 'Name':
                continue
            
            try:
                year_val = row.get('Year')
                year = None
                if not pd.isna(year_val):
                    try:
                        year = int(float(year_val))
                    except (ValueError, TypeError):
                        pass
                
                pos_val = row.get('Position')
                position = None
                if not pd.isna(pos_val):
                    try:
                        position = int(float(pos_val))
                    except (ValueError, TypeError):
                        pass
                
                film = {
                    'name': name_str,
                    'year': year,
                    'country': desc_str,
                    'url': str(row['URL']).strip() if not pd.isna(row.get('URL')) else None,
                    'position': position
                }
                films.append(film)
            except Exception as e:
                logger.debug(f"Skipping row due to error: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Error parsing CSV: {e}")
        raise
    
    return films


def _find_latest_database_file() -> Optional[str]:
    """Find the most recent all_the_films database file in output directory."""
    output_dir = Path('output')
    if not output_dir.exists():
        return None
    
    # Look for files matching pattern
    patterns = [
        'all_the_films_ratings_stats_*.json',
        'all_the_films_ratings_stats_*.csv',
        'all_the_films_ratings_stats.json',
        'all_the_films_ratings_stats.csv'
    ]
    
    candidates = []
    for pattern in patterns:
        candidates.extend(output_dir.glob(pattern))
    
    if not candidates:
        return None
    
    # Return most recently modified
    return str(max(candidates, key=lambda p: p.stat().st_mtime))


def _load_film_database(file_path: str) -> List[Dict[str, Any]]:
    """Load film database from JSON or CSV."""
    path = Path(file_path)
    
    if path.suffix == '.json':
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    elif path.suffix == '.csv':
        df = pd.read_csv(path, encoding='utf-8')
        return df.to_dict('records')
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}")


def _find_film_rating(film_name: str, year: Optional[int], all_films: List[Dict[str, Any]]) -> Optional[float]:
    """Find a film's rating in the database."""
    # Normalize the search name for comparison
    search_name = film_name.strip().lower()
    
    for film in all_films:
        # Try both 'name' and 'title' fields
        db_name = (film.get('name') or film.get('title') or '').strip().lower()
        db_title = (film.get('title') or film.get('name') or '').strip().lower()
        
        # Check if names match (exact or partial)
        name_match = (
            db_name == search_name or 
            db_title == search_name or
            search_name in db_name or
            search_name in db_title
        )
        
        if name_match:
            # If year is provided, verify it matches
            if year is not None:
                db_year = film.get('year')
                if db_year is not None:
                    try:
                        if int(db_year) != int(year):
                            continue
                    except (ValueError, TypeError):
                        pass
            
            # Return rating
            rating = film.get('average_rating') or film.get('rating')
            if rating is not None:
                try:
                    return float(rating)
                except (ValueError, TypeError):
                    pass
    
    return None


def _find_better_films_by_country(country: str, min_rating: float, all_films: List[Dict[str, Any]], 
                                  min_diff: float = 0.01) -> List[Dict[str, Any]]:
    """Find all films from a country with rating higher than minimum."""
    better_films = []
    
    # Normalize country name for comparison
    country_normalized = country.strip().lower()
    
    for film in all_films:
        # Check if film is from this country
        countries = film.get('countries', [])
        
        # Handle different formats
        if isinstance(countries, str):
            # Could be string representation of list like "['Japan']" or just "Japan"
            if countries.startswith('['):
                # String representation of list - parse it
                import ast
                try:
                    countries = ast.literal_eval(countries)
                except:
                    countries = [c.strip() for c in countries.strip('[]').replace("'", "").split(',')]
            else:
                countries = [c.strip() for c in countries.split(',')]
        
        # Normalize all country names and check for match
        countries_normalized = [c.strip().lower() for c in countries if c]
        
        if country_normalized not in countries_normalized:
            continue
        
        # Check rating
        rating = film.get('average_rating') or film.get('rating')
        if rating is None:
            continue
        
        try:
            rating = float(rating)
            if rating >= min_rating + min_diff:
                better_films.append(film)
        except (ValueError, TypeError):
            continue
    
    # Sort by rating descending
    better_films.sort(key=lambda x: float(x.get('average_rating', x.get('rating', 0))), reverse=True)
    
    return better_films


# ==================== LIST-TO-JSON COMMANDS ====================

def _parse_letterboxd_list_url(url: str):
    """Parse a Letterboxd list URL into (username, list_slug).
    
    Accepts formats like:
      https://letterboxd.com/user/list/slug/
      https://letterboxd.com/user/list/slug/page/3/
      user/list/slug
    
    Returns:
        Tuple of (username, list_slug) or raises ValueError
    """
    import re
    url = url.strip().rstrip('/')
    
    # Remove page suffix
    url = re.sub(r'/page/\d+$', '', url)
    
    # Full URL
    match = re.search(r'letterboxd\.com/([^/]+)/list/([^/]+)', url)
    if match:
        return match.group(1), match.group(2)
    
    # Short form: user/list/slug
    match = re.match(r'^([^/]+)/list/([^/]+)$', url)
    if match:
        return match.group(1), match.group(2)
    
    raise ValueError(
        f"Cannot parse Letterboxd list URL: {url}\n"
        f"Expected format: https://letterboxd.com/USERNAME/list/LIST_SLUG/"
    )


def _scrape_list_to_simple_json(scraper, url: str, workers: Optional[int] = None) -> List[Dict[str, Any]]:
    """Scrape a Letterboxd list and return simple [{id, name}, ...] data.
    
    Uses parallel page scraping via the existing framework.
    """
    username, list_slug = _parse_letterboxd_list_url(url)
    
    click.echo(f"📝 Scraping list: {username}/{list_slug}")
    
    # Get pagination info
    pagination_info = scraper.get_list_pagination_info(username, list_slug)
    total_pages = pagination_info['total_pages']
    estimated_films = pagination_info['total_films_estimate']
    
    click.echo(f"📊 Found {total_pages} page(s) with ~{estimated_films} films")
    
    if total_pages > 1:
        # Parallel scraping
        import multiprocessing
        max_workers = workers or min(8, multiprocessing.cpu_count())
        click.echo(f"🚀 Scraping {total_pages} pages in parallel ({max_workers} workers)")
        
        with tqdm(total=total_pages, desc="Pages", unit="page") as pbar:
            def progress_cb(completed, total, msg):
                pbar.n = completed
                pbar.refresh()
            
            films = scraper.get_all_films_from_list_parallel(
                username, list_slug, max_workers=max_workers,
                page_progress_callback=progress_cb
            )
    else:
        click.echo("📄 Single page — scraping directly")
        films = scraper.get_films_from_list(username, list_slug)
    
    # Convert to simple format: [{id, name}, ...]
    # Strip trailing " (YYYY)" year suffix from names to match existing ratings format
    import re as _re
    simple_films = []
    for film in films:
        film_id = film.get('film_id')
        name = film.get('name', 'Unknown')
        
        # Remove trailing year like " (2023)" but not mid-name years like "District 9 (2009)"
        # where name itself contains parenthesized text earlier
        name = _re.sub(r'\s+\(\d{4}\)$', '', name)
        
        if film_id is not None:
            try:
                film_id = int(film_id)
            except (ValueError, TypeError):
                pass
        
        simple_films.append({"id": film_id, "name": name})
    
    return simple_films


@scrape.command(name='list-to-json')
@click.argument('url')
@click.option('--output', '-o', default=None, help='Output JSON file path (overrides --output-dir and auto-naming)')
@click.option('--output-dir', '-d', default=None,
              help='Output directory (default: ../ratings/ relative to scrape_scripts)')
@click.option('--filename', help='Custom filename without extension (default: derived from list slug)')
@click.option('--workers', type=int, default=None, help='Number of parallel workers (default: auto)')
@click.option('--verbose', is_flag=True, help='Show detailed logging')
@click.pass_context
def list_to_json(ctx, url, output, output_dir, filename, workers, verbose):
    """
    Scrape a Letterboxd list and output a simple JSON file of film IDs and names.
    
    URL: Full Letterboxd list URL (e.g. https://letterboxd.com/user/list/slug/)
    
    Output format: [{"id": 12345, "name": "Film Name"}, ...]
    
    This is a lightweight replacement for the old notebook-based scraper,
    using cloudscraper to bypass Cloudflare protection.
    
    \b
    Examples:
      python main.py scrape list-to-json "https://letterboxd.com/dave/list/official-top-250-narrative-feature-films/"
      python main.py scrape list-to-json "https://letterboxd.com/jack/list/official-top-250-films-with-the-most-fans/" -o top_250_fans.json
      python main.py scrape list-to-json "https://letterboxd.com/el_duderinno/list/letterboxds-top-250-highest-rated-french-1/" --filename top_250_french
    """
    scraper = ctx.obj['scraper']
    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
    
    try:
        simple_films = _scrape_list_to_simple_json(scraper, url, workers)
        
        if not simple_films:
            click.echo("❌ No films found!")
            return
        
        click.echo(f"✅ Extracted {len(simple_films)} films")
        
        # Determine output path
        if output:
            out_path = Path(output)
        else:
            if output_dir:
                out_dir = Path(output_dir)
            else:
                # Default: ../ratings/ relative to scrape_scripts
                out_dir = Path(__file__).parent.parent / 'ratings'
            
            out_dir.mkdir(parents=True, exist_ok=True)
            
            if filename:
                out_path = out_dir / f"{filename}.json"
            else:
                # Derive from list slug
                _, list_slug = _parse_letterboxd_list_url(url)
                out_path = out_dir / f"{list_slug}.json"
        
        # Ensure parent dir exists
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(simple_films, f, ensure_ascii=False)
        
        click.echo(f"💾 Saved to: {out_path}")
        
        # Show first few films
        show_count = min(5, len(simple_films))
        click.echo(f"\n🎬 First {show_count} films:")
        for film in simple_films[:show_count]:
            click.echo(f"   {film['id']:>8}  {film['name']}")
        
    except ValueError as e:
        click.echo(f"❌ {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)
    finally:
        CLIHelper.cleanup_temp_verbose_logging(temp_verbose)


@cli.command(name='batch-scrape-lists')
@click.option('--config', '-c', default='configs/list_to_json_config.json',
              help='Path to JSON config file listing URLs to scrape (default: configs/list_to_json_config.json)')
@click.option('--output-dir', '-d', default=None,
              help='Output directory (default: ../ratings/ relative to scrape_scripts)')
@click.option('--workers', type=int, default=None, help='Parallel workers per list (default: auto)')
@click.option('--verbose', is_flag=True, help='Show detailed logging')
@click.pass_context
def batch_scrape_lists(ctx, config, output_dir, workers, verbose):
    """
    Batch-scrape multiple Letterboxd lists into simple JSON files.
    
    Reads a JSON config file containing a list of URLs and output filenames,
    then scrapes each one sequentially (pages within each list are parallel).
    
    \b
    Config format (configs/list_to_json_config.json):
    {
      "lists": [
        {
          "url": "https://letterboxd.com/user/list/slug/",
          "filename": "output_name"
        }
      ]
    }
    
    \b
    Examples:
      python main.py batch-scrape-lists
      python main.py batch-scrape-lists --config my_lists.json --output-dir ./my_output
    """
    temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
    
    try:
        # Load config
        config_path = Path(config)
        if not config_path.is_absolute():
            config_path = Path(__file__).parent / config_path
        
        if not config_path.exists():
            click.echo(f"❌ Config file not found: {config_path}")
            click.echo(f"💡 Create one at {config} — see 'python main.py batch-scrape-lists --help' for format")
            sys.exit(1)
        
        with open(config_path, 'r', encoding='utf-8') as f:
            batch_config = json.load(f)
        
        lists = batch_config.get('lists', [])
        if not lists:
            click.echo("⚠️  No lists found in config file")
            return
        
        click.echo(f"📋 Found {len(lists)} list(s) to scrape")
        
        # Determine output directory
        if output_dir:
            out_dir = Path(output_dir)
        else:
            out_dir = Path(batch_config.get('output_dir', str(Path(__file__).parent.parent / 'ratings')))
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Create scraper once, reuse for all lists
        scraper = LetterboxdScraper()
        
        successful = 0
        failed = []
        
        for i, list_entry in enumerate(lists, 1):
            list_url = list_entry.get('url', '')
            list_filename = list_entry.get('filename', '')
            
            if not list_url:
                click.echo(f"⚠️  [{i}/{len(lists)}] Skipping entry with no URL")
                failed.append(("(no url)", "Missing URL"))
                continue
            
            click.echo(f"\n{'='*50}")
            click.echo(f"📦 [{i}/{len(lists)}] {list_filename or list_url}")
            click.echo(f"{'='*50}")
            
            try:
                simple_films = _scrape_list_to_simple_json(scraper, list_url, workers)
                
                if not simple_films:
                    click.echo(f"⚠️  No films found for {list_url}")
                    failed.append((list_filename or list_url, "No films found"))
                    continue
                
                # Determine output filename
                if list_filename:
                    out_path = out_dir / f"{list_filename}.json"
                else:
                    _, list_slug = _parse_letterboxd_list_url(list_url)
                    out_path = out_dir / f"{list_slug}.json"
                
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(simple_films, f, ensure_ascii=False)
                
                click.echo(f"✅ {len(simple_films)} films → {out_path}")
                successful += 1
                
            except Exception as e:
                click.echo(f"❌ Failed: {e}")
                failed.append((list_filename or list_url, str(e)))
        
        # Summary
        click.echo(f"\n{'='*50}")
        click.echo(f"📊 Batch complete: {successful}/{len(lists)} succeeded")
        if failed:
            click.echo(f"❌ Failed ({len(failed)}):")
            for name, reason in failed:
                click.echo(f"   • {name}: {reason}")
        
    except json.JSONDecodeError as e:
        click.echo(f"❌ Invalid JSON in config file: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)
    finally:
        CLIHelper.cleanup_temp_verbose_logging(temp_verbose)


if __name__ == "__main__":
    cli()
