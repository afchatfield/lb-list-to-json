# Batch List Creation

This feature allows you to create multiple film lists automatically using predefined configurations. Instead of running the list creation command multiple times with different parameters, you can define all your desired lists in a JSON configuration file and run them all at once.

## Quick Start

1. **Create a template configuration file:**
   ```bash
   python main.py batch create-template
   ```

2. **Edit the configuration file** (`configs/batch_configurations_template.json`) to add your desired lists.

3. **Run all configurations:**
   ```bash
   python main.py batch run \
     --source-files "ratings/top_2000_highest_rated.json" \
     --source-files "ratings/top_2000_most_watched.json" \
     --output-dir "output/country_lists"
   ```

## Configuration Format

Each configuration in the JSON file can include the following fields:

### Required Fields
- `name`: Unique identifier for the configuration
- `title`: Display title for the list
- `output_filename`: Base name for output files (without extension)

### Optional Fields
- `description`: List description
- `limit`: Number of films to include (default: 50)
- `sort_by`: Field to sort by (`average_rating`, `release_year`, `runtime`, `name`)
- `sort_ascending`: Sort direction (default: false)

### Filter Fields
- `countries`: Array of country names (e.g., `["Japan", "South Korea"]`)
- `languages`: Array of language names (e.g., `["Japanese", "Korean"]`)
- `genres`: Array of genre names (e.g., `["Romance", "Comedy"]`)
- `min_year`, `max_year`: Year range filters
- `min_runtime`, `max_runtime`: Runtime filters in minutes
- `min_rating`, `max_rating`: Rating range filters (0-10 scale)
- `cutoff`: Minimum threshold type (`ratings` or `watches`)
- `cutoff_limit`: Minimum number for the cutoff
- `include_secondary_languages`: Include secondary languages in filter (default: false)

## Example Configuration

```json
{
  "configurations": [
    {
      "name": "top_250_japan",
      "title": "Top 250 Japanese Films",
      "description": "Top 250 Japanese films based on Letterboxd ratings with minimum 2000 ratings",
      "limit": 250,
      "countries": ["Japan"],
      "languages": ["Japanese"],
      "cutoff": "ratings",
      "cutoff_limit": 2000,
      "sort_by": "average_rating",
      "output_filename": "top_250_japanese_films"
    },
    {
      "name": "top_100_korea",
      "title": "Top 100 Korean Films",
      "description": "Top 100 Korean films based on watches",
      "limit": 100,
      "countries": ["South Korea"],
      "languages": ["Korean"],
      "cutoff": "watches",
      "cutoff_limit": 1000,
      "sort_by": "average_rating",
      "output_filename": "top_100_korean_films"
    },
    {
      "name": "modern_romance",
      "title": "Modern Romance Films",
      "description": "Top romance films from the last 20 years",
      "limit": 100,
      "genres": ["Romance"],
      "min_year": 2004,
      "max_year": 2024,
      "min_rating": 7.0,
      "sort_by": "average_rating",
      "output_filename": "modern_romance_films"
    }
  ]
}
```

## Commands

### List Available Configurations
```bash
python main.py batch list-configs
```
Shows all configurations in your batch config file.

### Create Template
```bash
python main.py batch create-template --output configs/my_lists.json
```
Creates a template configuration file with examples.

### Run Specific Configurations
```bash
python main.py batch run \
  --source-files "ratings/top_2000_highest_rated.json" \
  --specific-configs "top_250_japan,top_100_korea"
```
Runs only the specified configurations.

### Dry Run
```bash
python main.py batch run \
  --source-files "ratings/top_2000_highest_rated.json" \
  --dry-run
```
Shows what would be created without actually creating files.

### Custom Output Options
```bash
python main.py batch run \
  --source-files "ratings/top_2000_highest_rated.json" \
  --output-dir "my_custom_output" \
  --format csv
```

## Use Cases

This feature is perfect for:

1. **Country/Language Lists**: Creating top films lists for multiple countries
2. **Genre Collections**: Generating lists for different genres with consistent criteria
3. **Regular Updates**: Re-running the same set of lists when you get new data
4. **Bulk Processing**: Creating many lists with slight variations in parameters

## Tips

- Use descriptive `name` fields to easily identify configurations
- Set appropriate `cutoff_limit` values to ensure quality (higher for popular countries/genres)
- Use `dry-run` first to preview what will be created
- Keep source files updated for the best results
- Use `--specific-configs` to test individual configurations before running all
