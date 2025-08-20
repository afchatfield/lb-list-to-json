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

```bash
# Activate virtual environment
source env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the CLI framework
python main.py
```

## Development Roadmap

### Phase 1: Core Framework ✅
- [x] Base classes (Session, Scraper, Parser)
- [x] CLI framework structure
- [x] Configuration system
- [x] Project scaffolding

### Phase 2: Letterboxd Scrapers
- [ ] Film list scraper
- [ ] Individual film scraper
- [ ] User profile scraper
- [ ] Custom URL scraper

### Phase 3: Data Processing
- [ ] Film data parser
- [ ] List data parser
- [ ] Statistics parser
- [ ] Export utilities

### Phase 4: Advanced Features
- [ ] Selenium integration
- [ ] Database connectivity
- [ ] Batch processing
- [ ] API endpoints

### Phase 5: Production Features
- [ ] Error recovery
- [ ] Monitoring & alerts
- [ ] Performance optimization
- [ ] Documentation

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
