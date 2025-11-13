# Movie Recommendation API

FastAPI-based REST API for movie recommendations using Item-Based Collaborative Filtering. Can run locally for development or be deployed as an AWS Lambda function.

## Features

- **Item-Based Collaborative Filtering**: High-performance recommendation engine (RMSE: 0.8492, MAE: 0.6501)
- **Hybrid Onboarding Strategy**: Adapts recommendations based on user rating history
  - 0 ratings: Popular movies
  - 1-4 ratings: Blend of personalized + popular
  - 5+ ratings: Fully personalized recommendations
- **TMDB Integration**: Enriches recommendations with movie posters and overviews from The Movie Database
- **FastAPI**: Modern, fast, with automatic interactive documentation
- **Lambda-ready**: Uses Mangum adapter for AWS Lambda deployment

## Project Structure

```
movie-engine-api/
├── main.py              # FastAPI application and endpoints
├── model_loader.py      # Loads and caches the ML model
├── recommender.py       # Recommendation engine logic
├── tmdb_client.py       # TMDB API integration
├── requirements.txt     # Python dependencies
├── run_local.py         # Local development server
├── test_api.py          # API testing script
├── test_tmdb.py         # TMDB integration test
├── TMDB_SETUP.md        # TMDB configuration guide
└── README.md            # This file
```

## Prerequisites

- Python 3.8+
- **Local Development:**
  - Pre-trained model files in `../movie-engine-data/models/`:
    - `item_similarity_matrix.pkl`
    - `user_item_matrix.pkl`
    - `item_based_metadata.json`
  - Processed data in `../movie-engine-data/processed/ml-100k/`:
    - `movies.csv`
    - `ratings.csv`
- **Lambda Deployment:**
  - S3 bucket named `movie-engine-data` (or set `S3_BUCKET_NAME` env var)
  - Model files uploaded to S3 (see deployment section below)

## Installation

1. **Install dependencies:**

```bash
pip install -r requirements.txt
```

## Running Locally

### Configure TMDB API Key (Optional)

To enable movie posters and overviews, set your TMDB API key:

```bash
export TMDB_API_KEY="your_api_key_here"
```

See [TMDB_SETUP.md](TMDB_SETUP.md) for detailed instructions on obtaining and configuring a TMDB API key.

**Note:** The API works without a TMDB key, but `poster_url` and `overview` fields will be `null`.

### Start the API server:

```bash
python run_local.py
```

The API will be available at:
- API: http://localhost:8000
- Interactive docs (Swagger UI): http://localhost:8000/docs
- Alternative docs (ReDoc): http://localhost:8000/redoc

### Test the API:

In a separate terminal, run:

```bash
python test_api.py
```

Or use curl:

```bash
# Health check
curl http://localhost:8000/health

# Get recommendations for new user (no ratings)
curl -X POST http://localhost:8000/recommendations \
  -H "Content-Type: application/json" \
  -d '{"user_ratings": {}, "n": 5}'

# Get recommendations with user ratings
curl -X POST http://localhost:8000/recommendations \
  -H "Content-Type: application/json" \
  -d '{
    "user_ratings": {
      "1": 5.0,
      "50": 4.5,
      "32": 2.0
    },
    "n": 10,
    "k": 10
  }'
```

## API Endpoints

### GET `/`
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "service": "Movie Recommendation API",
  "version": "1.0.0"
}
```

### GET `/health`
Detailed health check with model status.

**Response:**
```json
{
  "status": "healthy",
  "model_loaded": true,
  "model_type": "item_based_collaborative_filtering",
  "num_movies": 9742
}
```

### POST `/recommendations`
Get movie recommendations based on user ratings.

**Request Body:**
```json
{
  "user_ratings": {
    "1": 5.0,
    "50": 4.5,
    "32": 2.0
  },
  "n": 10,
  "k": 10
}
```

**Parameters:**
- `user_ratings` (required): Dictionary of movieId to rating (0.5-5.0). Can be empty `{}` for new users.
- `n` (optional): Number of recommendations to return (default: 10, range: 1-100)
- `k` (optional): Number of similar items to consider (default: 10, range: 1-50)

**Response:**
```json
{
  "recommendations": [
    {
      "movieId": 318,
      "title": "Shawshank Redemption, The (1994)",
      "predicted_rating": 4.85,
      "poster_url": "https://image.tmdb.org/t/p/w500/q6y0Go1tsGEsmtFryDOJo3dEmqu.jpg",
      "overview": "Framed in the 1940s for the double murder of his wife and her lover, upstanding banker Andy Dufresne begins a new life at the Shawshank prison..."
    },
    ...
  ],
  "total": 10,
  "strategy": "personalized"
}
```

**Note:** `poster_url` and `overview` are only populated when TMDB_API_KEY is configured.

**Strategy Types:**
- `popular`: Used when no ratings provided (cold start)
- `hybrid`: Used for 1-4 ratings (blend of personalized + popular)
- `personalized`: Used for 5+ ratings (fully item-based CF)

## Deploying to AWS Lambda

The API automatically detects when running in Lambda (via `AWS_LAMBDA_FUNCTION_NAME` environment variable) and loads files from S3 instead of the local filesystem.

### Using AWS CDK (Recommended)

The easiest way to deploy is using the AWS CDK stack in the `/infra` folder:

```bash
cd ../infra

# Install CDK dependencies
pip install -r requirements.txt

# Deploy everything (Lambda, API Gateway, S3, IAM roles)
cdk deploy

# Upload model files to S3
aws s3 sync ../movie-engine-data/models s3://movie-engine-data/models
aws s3 sync ../movie-engine-data/processed s3://movie-engine-data/processed
```

See [`../infra/README.md`](../infra/README.md) for detailed deployment instructions, configuration options, and troubleshooting.

### Manual Deployment

If you prefer manual deployment without CDK, you'll need to:

1. Create an S3 bucket (`movie-engine-data`)
2. Upload model files to S3 with the same directory structure
3. Package the Lambda function with dependencies
4. Create IAM role with S3 read permissions
5. Deploy Lambda function (3GB memory, 60s timeout)
6. Configure API Gateway HTTP API

See the CDK stack in `/infra/movie_engine_api_stack.py` for reference configuration.

### Environment Variables

Configure in Lambda:
- `S3_BUCKET_NAME`: S3 bucket name (default: `movie-engine-data`)
- `TMDB_API_KEY`: The Movie Database API key (optional, for poster/overview data)
- Lambda automatically sets `AWS_LAMBDA_FUNCTION_NAME` for environment detection

### Performance Notes

- **Cold Start**: First request takes 10-30 seconds (downloading from S3)
- **Warm Requests**: Subsequent requests are fast (model cached in memory)
- **Memory**: Requires 3GB for large similarity matrix (~750MB)
- **Timeout**: Set to 60 seconds for cold start downloads

## Development

### Project Dependencies

- **fastapi**: Web framework
- **uvicorn**: ASGI server for local development
- **mangum**: ASGI adapter for AWS Lambda
- **pydantic**: Data validation
- **pandas**: Data manipulation
- **numpy**: Numerical operations

### Adding New Endpoints

Add new endpoints in `main.py`:

```python
@app.get("/movies/search")
async def search_movies(query: str):
    engine = get_recommendation_engine()
    # Your logic here
    return {"results": results}
```

### Running Tests

```bash
# Start the server
python run_local.py

# In another terminal
python test_api.py
```

## Model Information

- **Type**: Item-Based Collaborative Filtering
- **Dataset**: MovieLens 100K
- **Performance**: RMSE: 0.8492, MAE: 0.6501
- **Movies**: 9,742
- **Users**: 610
- **Ratings**: 100,836

## Troubleshooting

### Model files not found (local)
Ensure the model files are in the correct location:
```
../movie-engine-data/models/item_similarity_matrix.pkl
../movie-engine-data/models/user_item_matrix.pkl
../movie-engine-data/models/item_based_metadata.json
../movie-engine-data/processed/ml-100k/movies.csv
../movie-engine-data/processed/ml-100k/ratings.csv
```

### S3 Access Denied (Lambda)
- Verify Lambda execution role has S3 read permissions
- Check bucket name matches environment variable
- Verify files exist in S3: `aws s3 ls s3://movie-engine-data/models/`

### Memory errors
The item similarity matrix is large (~750MB). Ensure:
- Local: Sufficient system memory
- Lambda: At least 3GB memory configuration

### Import errors
Make sure all dependencies are installed:
```bash
pip install -r requirements.txt
```

### Lambda cold starts
First request after deployment may be slow (10-30s) as model loads from S3. Consider:
- Using Lambda provisioned concurrency
- Implementing lazy loading
- Using EFS for faster file access

### boto3 not found (local development)
boto3 is only needed for S3 access. Install it:
```bash
pip install boto3
```

For local development without S3, the API will automatically use local files.

## License

See main project LICENSE file.
