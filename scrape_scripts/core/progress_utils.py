"""
Progress bar utilities for enhanced tqdm formatting
"""

from tqdm import tqdm


def create_progress_bar(total=None, desc="Progress", position=0, leave=True, unit="it"):
    """
    Create a standardized tqdm progress bar with enhanced formatting.
    
    Args:
        total: Total number of items (None for unknown)
        desc: Description text
        position: Position for multiple progress bars
        leave: Whether to leave the bar after completion
        unit: Unit text (e.g., 'it', 'file', 'page')
    
    Returns:
        tqdm: Configured progress bar
    """
    return tqdm(
        total=total,
        desc=desc,
        position=position,
        leave=leave,
        unit=unit,
        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
        ncols=80,
        dynamic_ncols=True
    )


def create_dual_progress_bars(total_pages):
    """
    Create standardized dual progress bars for page and film processing.
    
    Args:
        total_pages: Total number of pages
    
    Returns:
        tuple: (page_pbar, film_pbar)
    """
    page_pbar = create_progress_bar(
        total=total_pages,
        desc="Pages",
        position=0,
        leave=True,
        unit="page"
    )
    
    film_pbar = create_progress_bar(
        total=None,
        desc="Films on page",
        position=1,
        leave=False,
        unit="film"
    )
    
    return page_pbar, film_pbar


def create_parallel_progress_bars():
    """
    Create standardized progress bars for parallel processing.
    
    Returns:
        tuple: (total_progress, detail_progress)
    """
    total_progress = create_progress_bar(
        desc="Overall progress",
        position=0,
        leave=True,
        unit="page"
    )
    
    detail_progress = create_progress_bar(
        desc="Getting details",
        position=1,
        leave=False,
        unit="film"
    )
    
    return total_progress, detail_progress
