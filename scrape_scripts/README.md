# Letterboxd Scraping Framework

A hierarchical, OOP-based framework for efficient web scraping of Letterboxd data with both JSON requests and Selenium support.

## Framework Structure

```
scrape_scripts/
├── main.py                    # Main CLI entry point
├── requirements.txt           # Python dependencies
├── core/                      # Base classes
│   ├── __init__.py
│   ├── base_session.py        # Session management & HTTP requests
│   ├── base_scraper.py        # HTML scraping with configurable selectors
│   └── base_parser.py         # Data cleaning & DataFrame conversion
├── scrapers/                  # Letterboxd-specific scrapers
│   └── __init__.py
├── parsers/                   # Data parsers for different content types
│   └── __init__.py
└── configs/                   # JSON configuration files
    └── letterboxd_selectors.json
```

## Core Components

### BaseSession
- Manages HTTP sessions with connection pooling
- Handles rate limiting and retry logic
- Configurable headers and timeouts
- Context manager support

### BaseScraper
- Uses JSON config files for HTML selectors
- Supports nested/recursive element extraction
- BeautifulSoup integration
- Flexible selector path system

### BaseParser
- Converts scraped data to pandas DataFrames
- Configurable data cleaning rules
- Validation and duplicate removal
- Custom cleaning function support

### CLI Framework
- Menu-driven interface
- Logging and error handling
- Modular command structure
- Configuration management

## Configuration System

HTML selectors are defined in JSON files under `configs/`. Example structure:

```json
{
  "film_list": {
    "container": ".js-list-entries",
    "film_item": "li",
    "film_data": {
      "id": "[data-film-id]",
      "name": "img",
      "link": "[data-target-link]"
    }
  }
}
```

## Usage

This is a **command-line interface (CLI)** tool where you specify the command you want to run directly when executing the script. It is **NOT** an interactive menu - you must provide the command and options as arguments.

```bash
# Activate virtual environment
source env/bin/activate

# Install dependencies
pip install -r requirements.txt

# View all available commands and options
python main.py --help

# View help for a specific command group
python main.py scrape --help
python main.py create --help
python main.py data --help
```

## Available Commands

### Main Command Groups

The CLI is organized into several command groups:

1. **`scrape`** - Extract data from Letterboxd lists and films
2. **`create`** - Generate new lists from existing data
3. **`data`** - Analyze and process film data
4. **`batch-lists`** - Create multiple lists automatically using predefined configurations
5. **`info`** - Display framework information
6. **`logs`** - View recent framework logs

### Scrape Commands

Extract data from Letterboxd:

```bash
# Scrape a single film's details
python main.py scrape film <film-slug> --output film_data.json

# Scrape basic info from a list (film IDs, names, years, etc.)
python main.py scrape list-basic <username> <list-slug> --output-dir output

# Scrape detailed info from a list (visits each film page)
python main.py scrape list-detailed <username> <list-slug> --parallel --output-dir output

# Scrape only ratings and stats from a list (fast)
python main.py scrape list-ratings <username> <list-slug> --output-dir output

# Use predefined lists (no need to specify username/list-slug)
python main.py scrape list-basic --predefined letterboxd_250 --output-dir output
```

**Scrape Options:**
- `--output-dir` / `-d`: Directory for output files (default: `output`)
- `--format` / `-f`: Output format - `json`, `csv`, or `both` (default: `both`)
- `--filename`: Custom filename (without extension)
- `--parallel`: Use parallel processing for faster scraping
- `--workers`: Number of parallel workers (auto-detected by default)
- `--predefined`: Use a predefined list key instead of username/list-slug
- `--verbose`: Show detailed logging output

### Create Commands

Generate new lists from existing data:

```bash
# Create a custom list from JSON data files
python main.py create list-from-files \
  --file "output/film_details.json" \
  --file "output/film_ratings.json" \
  --output-file "output/my_custom_list" \
  --format both \
  --title "My Custom Film List" \
  --limit 100 \
  --sort-by average_rating \
  --countries "USA,UK" \
  --cutoff ratings \
  --cutoff-limit 2000
```

**Create Options:**
- `--file` / `-f`: Path to JSON data file (can be used multiple times)
- `--output-file` / `-o`: Output path (without extension)
- `--format`: Output format - `json`, `csv`, or `both`
- `--title`: Title for the list
- `--description`: Description for the list
- `--limit`: Number of films to include
- `--sort-by`: Field to sort by (e.g., `average_rating`, `release_year`)
- `--sort-ascending`: Sort in ascending order
- `--countries`: Comma-separated list of countries to filter by
- `--languages`: Comma-separated list of languages to filter by
- `--genres`: Comma-separated list of genres to filter by
- `--min-year` / `--max-year`: Year range filter
- `--min-rating` / `--max-rating`: Rating range filter
- `--cutoff`: Apply minimum cutoff (`ratings` or `watches`)
- `--cutoff-limit`: Minimum number of ratings/watches required
- `--simple-json`: Output simple JSON array with film ID and name

### Batch Lists Command

Create multiple lists automatically from a configuration file:

```bash
# Run batch list creation with default config
python main.py batch-lists

# Use custom configuration file
python main.py batch-lists --config configs/my_batch_config.json

# Specify input files explicitly
python main.py batch-lists \
  --input-files output/detailed_list1.json \
  --input-files output/detailed_list2.json \
  --output-dir output/my_batch_lists
```

**Batch Lists Options:**
- `--config`: Path to batch configuration file (default: `configs/batch_configurations.json`)
- `--input-files` / `-i`: Paths to input JSON files (auto-detected if not provided)
- `--output-dir`: Output directory (default: `output/batch_lists`)
- `--simple-json` / `--no-simple-json`: Generate simple JSON format (default: enabled)

### Data Commands

Analyze and process film data:

```bash
# Convert JSON to pandas DataFrame (CSV)
python main.py data to-dataframe input.json --output output.csv --show-summary

# Generate summary statistics
python main.py data summary input.json --output summary.csv

# Analyze genre distribution
python main.py data genres input.json --output genres.csv --min-count 5

# Combine multiple JSON files
python main.py data combine file1.json file2.json file3.json --output combined.csv

# Create a filtered list by country/language
python main.py data create-list *.json --country france --language french --top 250

# Show dataset statistics
python main.py data dataset-info output/*.json

# Convert CSV to simple JSON format
python main.py data csv-to-simple-json output/batch_lists/my_list.csv
```

**Data Options:**
- `--output` / `-o`: Output file path
- `--clean`: Apply data cleaning (default: enabled)
- `--show-summary`: Display summary statistics
- `--min-count`: Minimum count to include in genre analysis
- `--country`: Filter by country
- `--language`: Filter by language
- `--top`: Number of films to return
- `--sort-by`: Field to sort by

### Info & Logs Commands

```bash
# Display framework information
python main.py info

# View recent logs
python main.py logs
```

### Global Options

These options work with any command:

- `--debug`: Enable debug logging
- `--verbose`: Show detailed logging output (default: quiet mode)
- `--log-file`: Specify log file path (default: `scraping.log`)

## Examples

### Example 1: Scrape a predefined list with detailed info

```bash
python main.py scrape list-detailed --predefined letterboxd_250 --parallel --output-dir output
```

### Example 2: Create a custom top 100 French films list

```bash
python main.py create list-from-files \
  --file "output/detailed_films.json" \
  --output-file "ratings/top_100_french_films" \
  --format both \
  --title "Top 100 French Films" \
  --limit 100 \
  --sort-by average_rating \
  --countries "France" \
  --cutoff ratings \
  --cutoff-limit 5000 \
  --simple-json
```

### Example 3: Batch create multiple regional lists

```bash
# First, ensure your configs/batch_configurations.json is set up
python main.py batch-lists --output-dir output/batch_lists
```

### Example 4: Analyze existing data

```bash
# Get dataset overview
python main.py data dataset-info output/*.json

# Convert to CSV for analysis
python main.py data combine output/*.json --output combined_films.csv
```

## Command Functionality Overview

### What Each Command Does

#### **Scrape Commands** - Extract data from Letterboxd.com
- **`scrape film`**: Downloads detailed information for a single film (director, cast, genres, runtime, ratings, etc.)
- **`scrape list-basic`**: Quickly scrapes basic film info from a list (film IDs, names, years, directors) - doesn't visit individual film pages
- **`scrape list-detailed`**: Comprehensively scrapes detailed info by visiting each film's page - slower but gets complete data
- **`scrape list-ratings`**: Fast extraction of only ratings and statistics (average rating, number of watches, etc.)

#### **Create Commands** - Generate new curated lists
- **`create list-from-files`**: Takes existing JSON data files and creates new filtered/sorted lists based on criteria like countries, languages, genres, ratings, year ranges, etc.

#### **Batch Lists** - Automated batch processing
- **`batch-lists`**: Reads a configuration file and automatically generates multiple lists at once (e.g., "Top 100 Korean Films", "Top 250 French Films", etc.)

#### **Data Commands** - Analyze and transform data
- **`data to-dataframe`**: Converts JSON film data to CSV format using pandas DataFrames
- **`data summary`**: Generates statistical summaries (total films, average ratings, year ranges, etc.)
- **`data genres`**: Analyzes and counts genre distribution across films
- **`data combine`**: Merges multiple JSON files into a single dataset and removes duplicates
- **`data create-list`**: Creates filtered lists by country/language from existing data
- **`data dataset-info`**: Shows comprehensive statistics about your film dataset
- **`data csv-to-simple-json`**: Converts CSV files to simple JSON format (just film ID and name)

#### **Utility Commands**
- **`info`**: Displays framework capabilities and available predefined lists
- **`logs`**: Shows recent log entries from scraping operations

### Database Support

**Currently: NO database commands exist.** All data is stored in files:
- JSON files (structured data with all film information)
- CSV files (tabular data for analysis in Excel/spreadsheet apps)

The framework currently uses **file-based storage only**. Data persistence is through:
- Reading/writing JSON files
- Exporting to CSV for analysis
- No SQL database, MongoDB, or other database systems

**Future plans** (Phase 4 in roadmap): Database connectivity is planned but not yet implemented.

## Development Roadmap

### Phase 1: Core Framework ✅
- [x] Base classes (Session, Scraper, Parser)
- [x] CLI framework structure
- [x] Configuration system
- [x] Project scaffolding

### Phase 2: Letterboxd Scrapers ✅
- [x] Film list scraper
- [x] Individual film scraper
- [x] Ratings/stats scraper
- [x] Parallel processing support

### Phase 3: Data Processing ✅
- [x] Film data parser
- [x] List data parser
- [x] Statistics parser
- [x] Export utilities (JSON, CSV)
- [x] Batch list creation

### Phase 4: Advanced Features (Planned)
- [ ] Selenium integration for dynamic content
- [ ] Database connectivity (postgres)
- [ ] API endpoints for data access
- [ ] User profile scraping

### Phase 5: Production Features (Planned)
- [ ] Error recovery and resume capability
- [ ] Monitoring & alerts
- [ ] Performance optimization
- [ ] Comprehensive documentation

## Architecture Principles

1. **Modularity**: Each component has a single responsibility
2. **Configurability**: HTML selectors and behavior defined in config files
3. **Extensibility**: Easy to add new scrapers and parsers
4. **Efficiency**: Session reuse and intelligent rate limiting
5. **Reliability**: Error handling and retry mechanisms
6. **Scalability**: Framework supports multiple concurrent operations

## Contributing

When adding new components:

1. Inherit from appropriate base classes
2. Add configuration files for new HTML structures
3. Implement required abstract methods
4. Add comprehensive error handling
5. Update documentation and tests
