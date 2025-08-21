"""
CLI Helper functions for common operations.
"""

import logging
import sys
import json
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime
import click

class CLIHelper:
    """Helper class for common CLI operations."""
    
    @staticmethod
    def setup_temp_verbose_logging(verbose: bool, global_verbose: bool) -> bool:
        """
        Setup temporary verbose logging for a command.
        
        Args:
            verbose: Command-level verbose flag
            global_verbose: Global verbose flag
            
        Returns:
            True if temporary logging was added, False otherwise
        """
        if verbose and not global_verbose:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
            logging.getLogger().addHandler(console_handler)
            return True
        return False
    
    @staticmethod
    def cleanup_temp_verbose_logging(temp_verbose: bool) -> None:
        """
        Clean up temporary verbose logging.
        
        Args:
            temp_verbose: Whether temporary logging was added
        """
        if temp_verbose:
            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                    root_logger.removeHandler(handler)
                    break
    
    @staticmethod
    def validate_predefined_list(list_key: str, predefined_lists: Dict[str, Tuple[str, str]]) -> Optional[Tuple[str, str]]:
        """
        Validate and get predefined list information.
        
        Args:
            list_key: The predefined list key
            predefined_lists: Dictionary of predefined lists
            
        Returns:
            Tuple of (username, list_slug) if valid, None otherwise
        """
        if list_key not in predefined_lists:
            available_lists = list(predefined_lists.keys())
            click.echo(f"❌ Unknown list key: {list_key}")
            click.echo(f"📋 Available predefined lists:")
            for key in available_lists:
                username, list_name = predefined_lists[key]
                click.echo(f"   {key}: {username}/{list_name}")
            return None
        
        return predefined_lists[list_key]
    
    @staticmethod
    def generate_filename(base_name: str, list_type: str, custom_filename: Optional[str] = None) -> str:
        """
        Generate output filename with timestamp.
        
        Args:
            base_name: Base name for the file
            list_type: Type of list (basic, detailed, ratings_stats)
            custom_filename: Custom filename if provided
            
        Returns:
            Generated filename
        """
        if custom_filename:
            return custom_filename
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{base_name}_{list_type}_{timestamp}"
    
    @staticmethod
    def save_films_data(films: list, output_path: Path, filename: str, format_choice: str) -> None:
        """
        Save films data to JSON and/or CSV.
        
        Args:
            films: List of film dictionaries
            output_path: Output directory path
            filename: Base filename
            format_choice: Format choice ('json', 'csv', 'both')
        """
        # Save JSON
        if format_choice in ['json', 'both']:
            json_path = output_path / f"{filename}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(films, f, indent=2, ensure_ascii=False)
            click.echo(f"💾 JSON saved to: {json_path}")
        
        # Save CSV
        if format_choice in ['csv', 'both']:
            from parsers.letterboxd_parser import create_letterboxd_dataframe
            df = create_letterboxd_dataframe(films, clean_data=True)
            csv_path = output_path / f"{filename}.csv"
            df.to_csv(csv_path, index=False, encoding='utf-8')
            click.echo(f"💾 CSV saved to: {csv_path}")
    
    @staticmethod
    def show_processing_info(parallel: bool, workers: Optional[int]) -> None:
        """
        Show processing mode information.
        
        Args:
            parallel: Whether parallel processing is enabled
            workers: Number of workers if specified
        """
        if parallel:
            import multiprocessing
            max_workers = workers or min(8, multiprocessing.cpu_count())
            click.echo(f"🚀 Using parallel processing with {max_workers} workers")
        else:
            click.echo("🐌 Using sequential processing")
    
    @staticmethod
    def show_summary(list_identifier: str, films: list, total_pages: int, 
                    output_path: Path, failed_count: int = 0) -> None:
        """
        Show summary information after scraping.
        
        Args:
            list_identifier: List identifier (key or username/slug)
            films: List of scraped films
            total_pages: Number of pages processed
            output_path: Output directory path
            failed_count: Number of failed operations
        """
        click.echo(f"\n📈 Summary:")
        click.echo(f"  List: {list_identifier}")
        click.echo(f"  Total films: {len(films)}")
        click.echo(f"  Pages processed: {total_pages}")
        if failed_count > 0:
            click.echo(f"  Failed details: {failed_count}")
        if films:
            # Show sample data if available
            sample_film = films[0]
            if 'average_rating' in sample_film:
                click.echo(f"  Sample data: {sample_film.get('name', 'Unknown')} - Rating: {sample_film.get('average_rating', 'N/A')}")
        click.echo(f"  Output directory: {output_path.absolute()}")


class ScrapingMode:
    """Enum-like class for scraping modes."""
    BASIC = "basic"
    DETAILED = "detailed"
    RATINGS_STATS = "ratings_stats"


def create_list_scraper_command(mode: str):
    """
    Factory function to create list scraper commands with different modes.
    
    Args:
        mode: Scraping mode (basic, detailed, ratings_stats)
        
    Returns:
        Click command function
    """
    def command_func(ctx, username, list_slug, output_dir, format, filename, 
                    parallel, workers, verbose, predefined=None, continue_on_error=False):
        """Generic list scraping command."""
        scraper = ctx.obj['scraper']
        
        # Setup logging
        temp_verbose = CLIHelper.setup_temp_verbose_logging(verbose, ctx.obj.get('verbose', False))
        
        try:
            # Handle predefined lists
            if predefined:
                list_info = CLIHelper.validate_predefined_list(predefined, scraper.PREDEFINED_LISTS)
                if not list_info:
                    return
                username, list_slug = list_info
                list_identifier = predefined
                click.echo(f"📝 Scraping predefined list: {predefined} ({username}/{list_slug})")
            else:
                list_identifier = f"{username}/{list_slug}"
                click.echo(f"📝 Scraping list: {username}/{list_slug}")
            
            # Mode-specific messages
            if mode == ScrapingMode.DETAILED:
                click.echo("⚠️  This will visit each film page and may take some time...")
            elif mode == ScrapingMode.RATINGS_STATS:
                click.echo("📊 Fast mode: Only collecting ratings and statistics data")
            
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
            
            # Execute scraping based on mode
            films = execute_scraping_by_mode(scraper, mode, username, list_slug, 
                                           parallel, workers, total_pages)
            
            click.echo(f"✅ Processed {len(films)} films")
            
            # Generate filename and save
            base_filename = CLIHelper.generate_filename(
                predefined or f"{username}_{list_slug}", mode, filename
            )
            CLIHelper.save_films_data(films, output_path, base_filename, format)
            
            # Show summary
            CLIHelper.show_summary(list_identifier, films, total_pages, output_path)
            
        except Exception as e:
            click.echo(f"❌ Error scraping list: {e}", err=True)
            sys.exit(1)
        finally:
            CLIHelper.cleanup_temp_verbose_logging(temp_verbose)
    
    return command_func


def execute_scraping_by_mode(scraper, mode: str, username: str, list_slug: str, 
                           parallel: bool, workers: Optional[int], total_pages: int) -> list:
    """
    Execute scraping based on the specified mode.
    
    Args:
        scraper: LetterboxdScraper instance
        mode: Scraping mode
        username: Letterboxd username
        list_slug: List slug
        parallel: Whether to use parallel processing
        workers: Number of workers
        total_pages: Total pages to process
        
    Returns:
        List of scraped films
    """
    from core.progress_utils import create_dual_progress_bars
    
    if mode == ScrapingMode.BASIC:
        # Set up progress bars
        page_pbar, film_pbar = create_dual_progress_bars(total_pages)
        
        def page_progress(current, total, message):
            page_pbar.set_description(f"Page {current}/{total}")
            page_pbar.update(1)
            film_pbar.reset()
        
        def film_progress(current, total, message):
            film_pbar.total = total
            film_pbar.set_description(f"Film {current}/{total}")
            film_pbar.update(1)
        
        try:
            if parallel:
                return scraper.get_all_films_from_list_parallel(
                    username, list_slug, workers, page_progress, film_progress)
            else:
                return scraper.get_all_films_from_list_paginated(
                    username, list_slug, page_progress, film_progress)
        finally:
            page_pbar.close()
            film_pbar.close()
    
    elif mode == ScrapingMode.DETAILED:
        # Set up two-phase progress bars
        phase1_progress, phase2_progress = create_dual_progress_bars(total_pages)
        
        def page_progress(current, total, message):
            phase1_progress.total = total
            phase1_progress.set_description(f"Phase 1 - Pages: {current}/{total}")
            phase1_progress.update(1)
        
        def film_progress(current, total, message):
            phase2_progress.total = total
            phase2_progress.set_description(f"Phase 2 - Details: {current}/{total} - {message}")
            phase2_progress.update(1)
        
        try:
            return scraper.get_all_films_optimized(
                username, list_slug, page_progress, film_progress, workers)
        finally:
            phase1_progress.close()
            phase2_progress.close()
    
    elif mode == ScrapingMode.RATINGS_STATS:
        # Set up progress bars for ratings/stats
        phase1_progress, phase2_progress = create_dual_progress_bars(total_pages)
        
        def page_progress(current, total, message):
            phase1_progress.total = total
            phase1_progress.set_description(f"Phase 1 - Pages: {current}/{total}")
            phase1_progress.update(1)
        
        def film_progress(current, total, message):
            phase2_progress.total = total
            phase2_progress.set_description(f"Phase 2 - Ratings/Stats: {current}/{total} - {message}")
            phase2_progress.update(1)
        
        try:
            films = scraper.get_all_films_ratings_stats_only(
                username, list_slug, page_progress, film_progress, workers)
            
            # Sort films by list position
            try:
                films.sort(key=lambda x: (
                    x.get('list_position') or 999999,
                    x.get('source_page', 999),
                    x.get('film_slug', 'zzz')
                ))
            except Exception as e:
                logging.warning(f"Could not sort films by position: {e}")
            
            return films
        finally:
            phase1_progress.close()
            phase2_progress.close()
    
    else:
        raise ValueError(f"Unknown scraping mode: {mode}")
