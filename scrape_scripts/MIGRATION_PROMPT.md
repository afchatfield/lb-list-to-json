# Letterboxd Scraper Migration: /csi/ Endpoints → Main Page Extraction

## Problem Summary
The Letterboxd ratings scraper is currently broken because **all `/csi/` endpoints now return 403 Forbidden** (blocked as of late 2025). The scraper was previously using these endpoints:
- `/csi/film/{slug}/ratings-summary/` - for average rating, total ratings, fans count
- `/csi/film/{slug}/stats/` - for watches, lists, likes

## Solution
Extract all ratings and stats data from the **main film page HTML** instead (`https://letterboxd.com/film/{slug}/`). We've confirmed through testing that:
- ✅ **Ratings data IS present in raw HTML** (no JavaScript required)
- ✅ **Performance is acceptable**: 0.66s per film, ~55 minutes for 40K films with 8 workers
- ✅ **All required data is extractable** from meta tags and page content

## Current Status
We've tested and verified extraction methods work, but the **scraper code needs updating** to use these new selectors.

---

## Data Extraction Methods Found

### 1. Average Rating (PRIMARY DATA)
**Location**: Twitter meta tag  
**Selector**: `<meta name="twitter:data2" content="4.69 out of 5">`  
**Extraction**: Parse the `content` attribute and extract the float (e.g., "4.69")


### 2. Ratings Count
**Location**: Link to ratings histogram page  
**Selector**: `<a href="/film/{slug}/ratings/">` (link contains "/ratings/")  
**Extraction**: Parse text like "1,234,567" or "1.2M" from link text

**Example Code**:
```python
ratings_link = soup.find('a', href=lambda x: x and '/ratings/' in x)
if ratings_link:
    text = ratings_link.get_text(strip=True).replace(',', '')
    # Handle formats: "1234567" or "1.2M"
```

### 3. Watches Count, Fans Count, Lists Count, Likes Count
load request html of main page and find corresponding identifiers to extract information

---

## Files That Need Modification

### 1. `/configs/letterboxd_selectors.json`
**Current State**: Contains selectors for /csi/ endpoints that are now broken

**Required Changes**:
Add/update selectors for main page extraction:

```json
{
  "ratings": {
    "average_rating_meta": {
      "tag": "meta",
      "attrs": {"name": "twitter:data2"},
      "extract": "content",
      "description": "Average rating from Twitter meta tag (e.g., '4.69 out of 5')"
    },
    "ratings_count_link": {
      "tag": "a",
      "attrs": {"href_contains": "/ratings/"},
      "extract": "text",
      "description": "Total ratings count from ratings page link"
    },
    "watches_link": {
      "tag": "a", 
      "attrs": {"href_contains": "/stats/watches/"},
      "extract": "text",
      "description": "Watches count from stats link"
    },
    "lists_link": {
      "tag": "a",
      "attrs": {"href_contains": "/lists/"},
      "extract": "text", 
      "description": "Lists count from lists link"
    },
    "likes_link": {
      "tag": "a",
      "attrs": {"href_contains": "/likes/"},
      "extract": "text",
      "description": "Likes/fans count from likes link"
    }
  }
}
```

### 2. `/core/letterboxd_utils.py` - `FilmDataExtractor` class
**Methods to Update**:

#### `extract_ratings_data(self, soup)`
Currently expects soup from `/csi/film/{slug}/ratings-summary/`

**Change to**:
extract ratings from main page with new identifiers

#### `extract_stats_data(self, soup)` 
Currently expects soup from `/csi/film/{slug}/stats/`
change to get from new identifiers

### 3. `/scrapers/letterboxd_scraper.py`
**Methods to Update**:

#### Remove/deprecate these methods:
- `get_film_ratings_soup()` - no longer works (403)
- `get_film_stats_soup()` - no longer works (403)

#### Update `get_all_films_ratings_stats_only()`:
**Current Flow**: 
1. Get basic films
2. Call `/csi/` endpoints for each film

**New Flow**:
1. Get basic films
2. Call **main film page** for each film
3. Extract ratings and stats from same page


### 4. `/core/parallel_processor.py`
**Method to Update**: `get_ratings_stats_parallel()`

**Current Issue**: Calls `get_film_ratings_soup()` and `get_film_stats_soup()` (both broken)

**Fix**: Call main page once instead of two /csi/ calls:

---

## Testing Checklist

After making changes, test:

1. **Single film extraction**: 
   ```bash
   python3 -c "from scrapers.letterboxd_scraper import LetterboxdScraper; s = LetterboxdScraper(); soup = s.get_film_soup('harakiri'); print(s.film_data_extractor.extract_ratings_data(soup))"
   ```

2. **Small list scrape**:
   ```bash
   python main.py scrape list-ratings https://letterboxd.com/some_user/list/test-list/ --max-films 10
   ```

3. **Large list scrape** (performance test):
   ```bash
   python main.py scrape list-ratings https://letterboxd.com/hershwin/list/all-the-movies/ --max-films 100
   ```

4. **Verify output CSV contains**:
   - `average_rating` column with float values
   - `total_ratings` column with integers
   - `watches`, `lists`, `fans_count` columns

---

## Performance Expectations

Based on testing with main page extraction:
- **Single film**: ~0.66s (0.58s network + 0.08s parsing)
- **40K films**: ~55 minutes with 8 workers
- **Data transfer**: ~225KB per film (~8.6GB for 40K films)
- **Parsing overhead**: Negligible (12% of total time)

---

## Additional Context

### Project Structure
- **CLI**: `main.py` - Click-based commands
- **Scraper**: `scrapers/letterboxd_scraper.py` - Main scraping logic
- **Utils**: `core/letterboxd_utils.py` - Extraction helpers
- **Config**: `configs/letterboxd_selectors.json` - CSS selectors
- **Parallel**: `core/parallel_processor.py` - Multi-threaded processing

### Why /csi/ Endpoints Broke
Letterboxd implemented access controls in late 2025:
- All `/csi/` endpoints return 403 Forbidden
- Tested various HTTP headers - all blocked
- Even browser-like headers with Referer/Sec-Fetch-* fail
- Main pages remain accessible (200 OK)

### Why Main Page Extraction Works
- Twitter/OpenGraph meta tags included for social sharing
- Data is in raw HTML (no JavaScript execution needed)
- Same performance characteristics with parallel processing
- More resilient (less likely to break again)

---

## Questions to Address

1. **Are there other stats** we need beyond watches/lists/likes?
get the main stats we were getting before (look at 2025 ratings file to see)
2. **Should we cache** the main page soup to avoid re-fetching?
no - extract from main page and then move on
3. **Error handling**: What if meta tag is missing?
IMPORTANT: try to extract all possible avenues for ratings
if it doesnt work on the first film, stop the code and log the error, explaining all methods failed and why

---

## Goal
Update the scraper to extract ratings/stats from main film pages instead of broken /csi/ endpoints, using the meta tag and link-based selectors identified through testing.
