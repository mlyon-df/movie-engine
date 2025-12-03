# Movie Engine

A full-stack movie recommendation system featuring item-based collaborative filtering, built with React, FastAPI, and AWS infrastructure. This project demonstrates modern ML engineering practices from data processing to production deployment.

**License:** This code is licensed under the BSD-3-Clause license. You're welcome to use it with attribution, though it's primarily an educational project.

![Python XKCD](python.png)

[https://xkcd.com/353/](https://xkcd.com/353/)

## Project Overview

The Movie Engine provides personalized movie recommendations through an intuitive mobile-first web interface. The system uses item-based collaborative filtering trained on the MovieLens dataset, with real-time recommendations that adapt as users rate movies.

### Key Features

- 🎬 **Smart Recommendations**: Item-based collaborative filtering (RMSE: 0.8492, MAE: 0.6501)
- 📱 **Mobile-First UI**: React interface optimized for phone browsers
- 🎭 **Rich Metadata**: Movie posters and descriptions via TMDB API
- ☁️ **Cloud-Ready**: AWS Lambda + API Gateway + S3 deployment
- 🔥 **Performance**: Lambda warming and provisioned concurrency options
- 📊 **Hybrid Strategy**: Adapts to user rating count (popular → personalized)

### Architecture

```
┌─────────────────┐
│   React SPA     │  Mobile-first frontend with localStorage
│  (S3 Website)   │  persistence and movie detail modals
└────────┬────────┘
         │ HTTPS
         ▼
┌─────────────────┐
│  API Gateway    │  HTTP API with CORS
│   (REST API)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│  Lambda         │─────▶│  S3 Bucket      │  Model files, datasets
│  FastAPI + ML   │      │  movie-engine   │  Loaded on cold start
└─────────────────┘      └─────────────────┘
```

## Repository Structure

```
movie-engine/
├── movie-engine-api/       # FastAPI backend service
│   ├── main.py             # API endpoints and Lambda handler
│   ├── recommender.py      # ML recommendation engine
│   ├── tmdb_client.py      # TMDB API integration
│   └── README.md           # Backend documentation
│
├── movie-engine-fe/        # React frontend application
│   ├── src/
│   │   ├── components/     # MovieCard, Drawer, Modal
│   │   └── services/       # API client, localStorage
│   └── README.md           # Frontend documentation
│
├── movie-engine-data/      # Datasets and ML models
│   ├── raw/                # Original MovieLens data
│   ├── processed/          # Cleaned data for training
│   └── models/             # Trained similarity matrices
│
├── infra/                  # AWS CDK infrastructure
│   ├── app.py              # CDK application
│   ├── movie_engine_api_stack.py
│   ├── deploy_frontend.sh  # Automated deployment
│   └── README.md           # Infrastructure documentation
│
├── notebooks/              # Jupyter notebooks for exploration
│   ├── exploration.ipynb   # Data analysis
│   ├── ai_model.ipynb      # Model development
│   └── production_model.ipynb
│
├── scripts/                # Data processing utilities
│   ├── onehot_movies.py    # Genre encoding
│   └── filter_low_activity_users.py
│
└── charts/                 # Performance visualizations
```


## Quick Start

### Prerequisites

- **Python 3.8+** for backend and data processing
- **Node.js 18+** and npm for frontend
- **AWS Account** (optional, for cloud deployment)
- **TMDB API Key** (optional but recommended) - [Get one free](https://www.themoviedb.org/settings/api)

### Local Development Setup

1. **Clone and install dependencies:**

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install frontend dependencies
cd movie-engine-fe
npm install
cd ..
```

2. **Get the datasets:**

Download and extract to `movie-engine-data/raw/`:

- [MovieLens 100K Dataset](https://files.grouplens.org/datasets/movielens/ml-latest-small.zip)
- [MovieLens 32M Dataset](https://files.grouplens.org/datasets/movielens/ml-32m.zip)
- More at [grouplens.org/datasets/movielens](https://grouplens.org/datasets/movielens/)

3. **Process data and train models:**

See the notebooks in `notebooks/` for data exploration and model training:
- `exploration.ipynb` - Initial data analysis
- `ai_model.ipynb` - Model experimentation
- `production_model.ipynb` - Final model generation

The trained models should be in `movie-engine-data/models/`.

4. **Run the backend:**

```bash
cd movie-engine-api
export TMDB_API_KEY="your_api_key_here"  # Optional
python run_local.py
```

API available at http://localhost:8000 (docs at `/docs`)

5. **Run the frontend:**

```bash
cd movie-engine-fe
echo "VITE_API_URL=http://localhost:8000" > .env
npm run dev
```

App available at http://localhost:5173

### Cloud Deployment

Deploy the complete stack to AWS:

```bash
cd infra

# Install CDK dependencies
pip install -r requirements.txt

# Bootstrap CDK (first time only)
cdk bootstrap

# Set TMDB API key
export TMDB_API_KEY="your_api_key_here"

# Deploy backend
cdk deploy MovieEngineAPIStack

# Upload model files
aws s3 sync ../movie-engine-data/models s3://movie-engine-data/models/
aws s3 sync ../movie-engine-data/processed s3://movie-engine-data/processed/

# Deploy frontend (auto-configures API URL)
./deploy_frontend.sh
```

See `infra/README.md` for detailed deployment documentation.


## Component Documentation

Each major component has detailed documentation:

- **[Backend API](movie-engine-api/README.md)**: FastAPI service, endpoints, local testing
- **[Frontend](movie-engine-fe/README.md)**: React app, components, storage
- **[Infrastructure](infra/README.md)**: AWS deployment, CDK stacks, configuration
- **[TMDB Setup](movie-engine-api/TMDB_SETUP.md)**: API key setup and integration

## Technology Stack

### Backend
- **Python 3.13** - Runtime
- **FastAPI** - REST API framework
- **Mangum** - AWS Lambda adapter
- **Pandas/NumPy** - Data processing
- **scikit-learn** - ML metrics

### Frontend
- **React 18.3.1** - UI framework
- **Vite 7.2.2** - Build tool
- **localStorage** - Client-side persistence

### Infrastructure
- **AWS CDK** - Infrastructure as code
- **AWS Lambda** - Serverless compute
- **API Gateway** - HTTP API
- **S3** - Static hosting + model storage
- **EventBridge** - Lambda warming
- **CloudWatch** - Logs and monitoring

### External Services
- **TMDB API** - Movie metadata (posters, descriptions)
- **MovieLens** - Training data (GroupLens Research)

## Model Performance

Item-Based Collaborative Filtering trained on MovieLens 100K:

- **RMSE**: 0.8492 (Root Mean Square Error)
- **MAE**: 0.6501 (Mean Absolute Error)
- **Strategy**: Cosine similarity with 10 nearest neighbors
- **Training**: ~100K ratings, 600+ users, 9,000+ movies

### Recommendation Strategy

The system uses a hybrid approach based on user activity:

| Ratings Count | Strategy | Description |
|--------------|----------|-------------|
| 0 | **Popular** | Top-rated movies by average rating |
| 1-4 | **Hybrid** | 50% personalized + 50% popular |
| 5+ | **Personalized** | Full item-based collaborative filtering |

This provides good recommendations even for cold-start users while leveraging the full model for active users.

## Development Workflow

### Working with Notebooks

```bash
jupyter notebook notebooks/
```

- `exploration.ipynb` - Analyze raw data, visualize patterns
- `ai_model.ipynb` - Experiment with different algorithms
- `production_model.ipynb` - Train final model, generate similarity matrices

### Data Processing Scripts

```bash
# One-hot encode movie genres
python scripts/onehot_movies.py

# Filter low-activity users from dataset
python scripts/filter_low_activity_users.py
```

### Testing the Backend

```bash
cd movie-engine-api

# Run unit tests
python test_api.py

# Test TMDB integration
python test_tmdb.py
```

### Building the Frontend

```bash
cd movie-engine-fe

# Development server with hot reload
npm run dev

# Production build
npm run build

# Preview production build
npm run preview
```

## Deployment Considerations

### Lambda Cold Starts

The Lambda function loads ~100MB of model data from S3 on cold starts, which can take 10-30 seconds. Two solutions:

**1. Scheduled Warming (Free tier friendly):**
```bash
export ENABLE_WARMING=true
export WARMING_RATE_MINUTES=5
cdk deploy MovieEngineAPIStack
```

**2. Provisioned Concurrency (Guaranteed performance):**
```bash
export ENABLE_PROVISIONED_CONCURRENCY=true
export PROVISIONED_CONCURRENCY_COUNT=1
cdk deploy MovieEngineAPIStack
```

See `infra/README.md` for detailed cold start optimization strategies.

### Estimated AWS Costs

Low traffic (100 requests/day):
- Lambda: ~$0.20/month
- API Gateway: ~$1.00/month
- S3 Storage: ~$0.02/month
- S3 Requests: ~$0.01/month
- CloudWatch Logs: ~$0.50/month

**Total: ~$2/month** for development/testing

Provisioned concurrency adds ~$35/month per instance.

## Data Requirements

The following directories should exist:

```
movie-engine-data/
├── raw/                    # Original datasets (not in git)
│   ├── ml-100k/
│   └── ml-32m/
├── processed/              # Cleaned data (not in git)
│   ├── ml-100k/
│   │   ├── movies.csv
│   │   ├── ratings.csv
│   │   └── links.csv
│   └── ml-32m/
└── models/                 # Trained models (not in git)
    ├── item_similarity_matrix.pkl
    ├── user_item_matrix.pkl
    └── item_based_metadata.json
```

**Note:** Data files are excluded from git due to size. Download from MovieLens and process using the notebooks.

## Contributing

This is an educational project, but contributions are welcome! Areas for improvement:

- [ ] Add user accounts and persistent ratings
- [ ] Implement matrix factorization models
- [ ] Add A/B testing framework
- [ ] Create recommendation explanations
- [ ] Add more movie metadata sources
- [ ] Expand the model to use tags and content features

## License

BSD-3-Clause License - See [LICENSE](LICENSE) file for details.

## Acknowledgments

- **GroupLens Research** - MovieLens datasets
- **The Movie Database (TMDB)** - Movie metadata API
- **xkcd** - For the Python comic

## Contact

Built as a learning project for Clemson University CPSC-8740. Feel free to explore, learn from it, and build your own version!
