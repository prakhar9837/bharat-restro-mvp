# Gold Standard Schema

This directory contains the gold standard dataset for evaluating the restaurant extraction pipeline.

## Files

- `gold_sample.csv`: Hand-curated restaurant data with verified information

## Schema

The gold standard CSV contains the following columns:

- **name**: Restaurant name (string)
- **phone**: Phone number in +91XXXXXXXXXX format (string)
- **address_full**: Complete address as a single string (string)
- **pincode**: 6-digit postal code (string)
- **lat**: Latitude coordinate (float)
- **lon**: Longitude coordinate (float)
- **website**: Official website URL or empty string (string)
- **cuisines**: JSON-encoded list of cuisine types from standard vocabulary (string)
- **hours_json**: JSON-encoded opening hours by day of week (string)

## Cuisine Vocabulary

Standard cuisine types used in the gold set:
- NORTH_INDIAN
- SOUTH_INDIAN
- CHINESE
- STREET_FOOD
- BAKERY
- CAFE
- ITALIAN
- MUGHLAI
- SEAFOOD

## Hours Format

Hours are stored as JSON with the following structure:
```json
{
  "monday": [{"open": "HH:MM", "close": "HH:MM"}],
  "tuesday": [{"open": "HH:MM", "close": "HH:MM"}],
  // ... other days
  "sunday": []  // empty array for closed days
}
```

Multiple time segments per day are supported (e.g., lunch and dinner).
Times are in 24-hour format.
