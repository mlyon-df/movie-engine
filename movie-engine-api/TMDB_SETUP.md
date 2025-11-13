# TMDB Integration Setup

## Overview
The Movie Engine API now integrates with The Movie Database (TMDB) to provide movie posters and overviews for all recommendations.

## Setting Up TMDB API Key

### 1. Get Your TMDB API Key
1. Go to [TMDB website](https://www.themoviedb.org/)
2. Create a free account or log in
3. Navigate to Settings → API
4. Request an API key (choose "Developer" option)
5. Fill out the form with your application details
6. You'll receive an API key (v3 auth)

### 2. Configure the API Key

#### For Local Development
Set the environment variable before running the API:

```bash
export TMDB_API_KEY="your_api_key_here"
python run_local.py
```

Or create a `.env` file in the `movie-engine-api` directory:
```
TMDB_API_KEY=your_api_key_here
```

#### For AWS Lambda Deployment
Add the TMDB_API_KEY as an environment variable in your Lambda function configuration:

1. Open AWS Lambda console
2. Select your function
3. Go to Configuration → Environment variables
4. Add key: `TMDB_API_KEY`, value: `your_api_key_here`

## API Response Format

### Before (without TMDB)
```json
{
  "movieId": 1,
  "title": "Toy Story (1995)",
  "predicted_rating": 4.8
}
```

### After (with TMDB)
```json
{
  "movieId": 1,
  "title": "Toy Story (1995)",
  "predicted_rating": 4.8,
  "poster_url": "https://image.tmdb.org/t/p/w500/uXDfjJbdP4ijW5hWSBrPrlKpxab.jpg",
  "overview": "Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene..."
}
```

## Features

### Caching
- TMDB API responses are cached using `@lru_cache` to minimize API calls
- Cache stores up to 1000 movie details
- Subsequent requests for the same movie use cached data

### Graceful Degradation
- If TMDB API key is not configured, the API still works
- `poster_url` and `overview` fields will be `null` in responses
- No errors or warnings are shown to end users

### Error Handling
- Network timeouts are handled gracefully (5 second timeout)
- 404 errors (movie not found) are logged but don't break the API
- All other errors are caught and logged

## Testing

Test the API with TMDB integration:

```bash
# Set your API key
export TMDB_API_KEY="your_api_key_here"

# Run the API
cd movie-engine-api
python run_local.py

# In another terminal, test the endpoint
curl -X POST http://localhost:8000/recommendations \
  -H "Content-Type: application/json" \
  -d '{"user_ratings": {1: 5.0, 50: 4.5}, "n": 5}'
```

## Rate Limits

TMDB API has rate limits:
- Free tier: 40 requests every 10 seconds
- The caching mechanism helps stay within these limits
- For production use with high traffic, consider implementing additional caching strategies (Redis, etc.)

## Image Sizes Available

TMDB provides posters in multiple sizes. The default is `w500` (500px width).
Available sizes:
- `w92` - 92px width (thumbnail)
- `w154` - 154px width (small)
- `w185` - 185px width (medium)
- `w342` - 342px width (large)
- `w500` - 500px width (default)
- `w780` - 780px width (extra large)
- `original` - Original size

To change the size, modify `IMAGE_BASE_URL` in `tmdb_client.py`.
