"""
Movie Recommendation API
FastAPI application for serving movie recommendations using Item-Based Collaborative Filtering.
Can run locally or be deployed as AWS Lambda with API Gateway.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from mangum import Mangum
import logging

from model_loader import ModelLoader
from recommender import RecommendationEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Movie Recommendation API",
    description="Item-Based Collaborative Filtering recommendation engine",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize model loader and recommendation engine (lazy loading)
model_loader = None
recommendation_engine = None


def get_recommendation_engine():
    """Get or initialize the recommendation engine (singleton pattern)"""
    global model_loader, recommendation_engine
    
    if recommendation_engine is None:
        logger.info("Initializing model loader and recommendation engine...")
        model_loader = ModelLoader()
        recommendation_engine = RecommendationEngine(
            item_similarity_df=model_loader.item_similarity_df,
            movies_df=model_loader.movies_df,
            ratings_df=model_loader.ratings_df,
            links_df=model_loader.links_df
        )
        logger.info("Recommendation engine initialized successfully!")
    
    return recommendation_engine


# Request/Response Models
class RecommendationRequest(BaseModel):
    """Request model for getting recommendations"""
    user_ratings: Dict[int, float] = Field(
        ...,
        description="Dictionary of movieId to rating (0.5-5.0). Can be empty dict for new users.",
        example={1: 5.0, 50: 4.5, 32: 2.0}
    )
    n: Optional[int] = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of recommendations to return"
    )
    k: Optional[int] = Field(
        default=10,
        ge=1,
        le=50,
        description="Number of similar items to consider"
    )


class MovieRecommendation(BaseModel):
    """Single movie recommendation"""
    movieId: int
    title: str
    predicted_rating: float
    poster_url: Optional[str] = Field(
        default=None,
        description="URL to movie poster from TMDB"
    )
    overview: Optional[str] = Field(
        default=None,
        description="Movie overview/description from TMDB"
    )


class RecommendationResponse(BaseModel):
    """Response model for recommendations"""
    recommendations: List[MovieRecommendation]
    total: int
    strategy: str = Field(
        description="Strategy used: 'popular', 'hybrid', or 'personalized'"
    )


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Movie Recommendation API",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check():
    """Detailed health check endpoint"""
    try:
        engine = get_recommendation_engine()
        return {
            "status": "healthy",
            "model_loaded": engine is not None,
            "model_type": "item_based_collaborative_filtering",
            "num_movies": len(engine.movies_df) if engine else 0
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unavailable")


@app.post("/recommendations", response_model=RecommendationResponse)
async def get_recommendations(request: RecommendationRequest):
    """
    Get movie recommendations based on user ratings.
    
    Strategy:
    - No ratings: Returns popular movies
    - 1-4 ratings: Blends personalized + popular recommendations
    - 5+ ratings: Fully personalized item-based collaborative filtering
    """
    try:
        # Validate ratings
        for movie_id, rating in request.user_ratings.items():
            if not (0.5 <= rating <= 5.0):
                raise HTTPException(
                    status_code=400,
                    detail=f"Rating for movieId {movie_id} must be between 0.5 and 5.0"
                )
        
        # Get recommendation engine
        engine = get_recommendation_engine()
        
        # Generate recommendations
        logger.info(f"Generating {request.n} recommendations for {len(request.user_ratings)} user ratings")
        recommendations_df = engine.recommend(
            user_ratings=request.user_ratings,
            n=request.n,
            k=request.k
        )
        
        # Determine strategy used
        num_ratings = len(request.user_ratings)
        if num_ratings == 0:
            strategy = "popular"
        elif num_ratings < 5:
            strategy = "hybrid"
        else:
            strategy = "personalized"
        
        # Convert to response format
        recommendations = [
            MovieRecommendation(
                movieId=int(row['movieId']),
                title=row['title'],
                predicted_rating=float(row['predicted_rating']),
                poster_url=row.get('poster_url'),
                overview=row.get('overview')
            )
            for _, row in recommendations_df.iterrows()
        ]
        
        logger.info(f"Successfully generated {len(recommendations)} recommendations using {strategy} strategy")
        
        return RecommendationResponse(
            recommendations=recommendations,
            total=len(recommendations),
            strategy=strategy
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate recommendations: {str(e)}"
        )


# Lambda handler (Mangum adapter)
# When deployed to Lambda, API Gateway will invoke this handler
handler = Mangum(app)


if __name__ == "__main__":
    # For local development only
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
