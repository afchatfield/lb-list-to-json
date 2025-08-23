# Dynamic Selector Framework Documentation

## Overview

The Letterboxd scraping framework now uses a dynamic selector system that loads all CSS selectors and HTML attributes from the `letterboxd_selectors.json` configuration file. This allows for easy modification of selectors without changing the code.

## Key Benefits

1. **Dynamic Configuration**: All selectors are loaded from JSON at runtime
2. **Easy Maintenance**: Update selectors by editing the JSON file, no code changes required
3. **Centralized Management**: All selectors in one place for better organization
4. **Fallback Support**: Multiple selector strategies for robustness
5. **Future-Proof**: Easy to adapt to Letterboxd website changes

## Configuration File Structure

The `letterboxd_selectors.json` file contains four main sections:

### 1. `film_list` - Selectors for list pages
```json
{
  "film_list": {
    "container": ".js-list-entries",
    "film_item": "li.poster-container", 
    "poster_container": ".poster-container",
    "data_item_slug": "div[data-item-slug]",
    "data_film_slug": "div[data-film-slug]",
    "data_film_id": "div[data-film-id]",
    "film_img": "img"
  }
}
```

### 2. `film_page` - Selectors for individual film pages
```json
{
  "film_page": {
    "title": "h1.headline-1 .name",
    "year": ".releasedate a",
    "director": ".credits .prettify",
    "cast": ".cast-list .text-slug",
    "genres": "#tab-genres .text-slug",
    "countries": "#tab-details .text-sluglist a[href*='/films/country/']"
  }
}
```

### 3. `pagination` - Selectors for pagination elements
```json
{
  "pagination": {
    "pagination_container": ".pagination",
    "page_links": ".pagination li a",
    "last_page": ".pagination li:last-child a"
  }
}
```

### 4. `attributes` - HTML attribute names for data extraction
```json
{
  "attributes": {
    "data_film_id": "data-film-id",
    "data_film_slug": "data-film-slug", 
    "data_item_slug": "data-item-slug",
    "data_owner_rating": "data-owner-rating"
  }
}
```

## How to Use

### Loading Configuration

The framework automatically loads the configuration using the `SelectorConfig` class:

```python
from core.config_loader import selector_config, get_selectors, get_selector

# Get all selectors
all_selectors = get_selectors()

# Get a specific selector using dot notation
title_selector = get_selector("film_page.title")

# Get category-specific selectors
film_list_selectors = selector_config.get_film_list_selectors()
```

### Using in Extractors

All extractor classes now accept selectors as parameters:

```python
# FilmDataExtractor with custom selectors
film_selectors = selector_config.get_film_page_selectors()
extractor = FilmDataExtractor(film_selectors)

# ListFilmExtractor with full config
list_extractor = ListFilmExtractor(selector_config.get_selectors())
```

### Multiple Selector Strategy

The framework uses multiple selectors for robustness:

```python
# Try modern selector first
film_posters = soup.select(selectors.get('data_item_slug', 'div[data-item-slug]'))

# Fallback to legacy selector
if not film_posters:
    film_posters = soup.select(selectors.get('data_film_slug', 'div[data-film-slug]'))
```

## Updated Classes

### 1. FilmDataExtractor
- Now accepts `selectors` parameter in constructor
- Uses film_page selectors from config
- Falls back to defaults if no config provided

### 2. ListFilmExtractor  
- Now accepts `selectors` parameter in constructor
- Uses film_list and attributes sections from config
- Multiple fallback strategies for different Letterboxd versions

### 3. PaginationHelper
- Now accepts `selectors` parameter in constructor
- Uses pagination selectors from config
- Handles pagination detection dynamically

### 4. LetterboxdScraper
- Loads selectors on initialization
- Passes appropriate selectors to each utility class
- Uses centralized config loader

## Testing Selectors

Use the validation script to test the selector configuration:

```bash
# Run basic validation
python validate_selectors.py

# Show current configuration
python validate_selectors.py --show-config
```

## Modifying Selectors

To update selectors for website changes:

1. Edit `configs/letterboxd_selectors.json`
2. Update the relevant selector
3. Run `python validate_selectors.py` to test
4. The framework will automatically use the new selectors

Example - updating the film title selector:
```json
{
  "film_page": {
    "title": "h1.new-title-class .film-name"
  }
}
```

## Error Handling

The framework includes robust error handling:

- **Missing Config**: Falls back to default selectors
- **Invalid JSON**: Logs error and uses defaults  
- **Missing Selectors**: Uses fallback strategies
- **Selector Not Found**: Returns None gracefully

## Best Practices

1. **Test Changes**: Always validate selectors after modifications
2. **Keep Fallbacks**: Maintain multiple selector strategies
3. **Document Updates**: Comment significant selector changes
4. **Version Control**: Track selector changes in git
5. **Monitor Logs**: Check for selector warnings in logs

## Troubleshooting

### Common Issues

1. **Selector Not Found**
   - Check JSON syntax in config file
   - Verify selector path using dot notation
   - Run validation script to identify issues

2. **No Data Extracted**  
   - Test selectors in browser console
   - Check if Letterboxd changed their HTML structure
   - Update selectors in JSON file

3. **Performance Issues**
   - Ensure selectors are specific enough
   - Avoid overly complex CSS selectors
   - Test with small lists first

### Debugging Commands

```python
# Check if selector exists
print(get_selector("film_page.title"))

# Reload configuration
selector_config.reload_config()

# Test in browser console
document.querySelector("h1.headline-1 .name")
```

## Migration Notes

The framework maintains backward compatibility by:
- Keeping default selectors in each class
- Supporting both old and new attribute names
- Graceful fallback when config fails to load

This ensures existing code continues to work while benefiting from the new dynamic system.
