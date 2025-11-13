"""
Test TMDB Integration
Simple test to verify TMDB data is being fetched and included in recommendations
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from model_loader import ModelLoader
from recommender import RecommendationEngine


def test_tmdb_integration():
    """Test that TMDB data is included in recommendations"""
    print("=" * 60)
    print("Testing TMDB Integration")
    print("=" * 60)
    
    # Check for API key
    api_key = os.environ.get('TMDB_API_KEY')
    if not api_key:
        print("\n⚠️  WARNING: TMDB_API_KEY not set!")
        print("Set it with: export TMDB_API_KEY='your_key_here'")
        print("Recommendations will work but won't include poster/overview\n")
    else:
        print(f"\n✓ TMDB_API_KEY is configured\n")
    
    # Load model and data
    print("Loading model and data...")
    model_loader = ModelLoader()
    
    print(f"✓ Loaded {len(model_loader.movies_df)} movies")
    print(f"✓ Loaded {len(model_loader.links_df)} movie links")
    
    # Initialize recommendation engine
    print("\nInitializing recommendation engine...")
    engine = RecommendationEngine(
        item_similarity_df=model_loader.item_similarity_df,
        movies_df=model_loader.movies_df,
        ratings_df=model_loader.ratings_df,
        links_df=model_loader.links_df
    )
    print("✓ Recommendation engine initialized")
    
    # Test 1: Get popular movies (for new users)
    print("\n" + "-" * 60)
    print("Test 1: Popular Movies (New User)")
    print("-" * 60)
    popular = engine.get_popular_movies(n=3)
    
    for idx, row in popular.iterrows():
        print(f"\n{idx + 1}. {row['title']}")
        print(f"   Rating: {row['predicted_rating']:.2f}")
        print(f"   Poster: {row['poster_url'] if row['poster_url'] else '❌ Not available'}")
        print(f"   Overview: {row['overview'][:80] + '...' if row['overview'] else '❌ Not available'}")
    
    # Test 2: Personalized recommendations
    print("\n" + "-" * 60)
    print("Test 2: Personalized Recommendations")
    print("-" * 60)
    
    # User likes Toy Story (1), Jumanji (2), and Heat (6)
    user_ratings = {1: 5.0, 2: 4.5, 6: 5.0, 50: 4.0, 32: 4.5}
    print(f"User has rated {len(user_ratings)} movies")
    
    recommendations = engine.recommend(user_ratings=user_ratings, n=3)
    
    for idx, row in recommendations.iterrows():
        print(f"\n{idx + 1}. {row['title']}")
        print(f"   Predicted Rating: {row['predicted_rating']:.2f}")
        print(f"   Poster: {row['poster_url'] if row['poster_url'] else '❌ Not available'}")
        print(f"   Overview: {row['overview'][:80] + '...' if row['overview'] else '❌ Not available'}")
    
    # Test 3: Similar movies
    print("\n" + "-" * 60)
    print("Test 3: Movies Similar to 'Toy Story' (movieId=1)")
    print("-" * 60)
    
    similar = engine.find_similar_movies(movie_id=1, n=3)
    
    for idx, row in similar.iterrows():
        print(f"\n{idx + 1}. {row['title']}")
        print(f"   Similarity: {row['similarity']:.3f}")
        print(f"   Poster: {row['poster_url'] if row['poster_url'] else '❌ Not available'}")
        print(f"   Overview: {row['overview'][:80] + '...' if row['overview'] else '❌ Not available'}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    has_posters = popular['poster_url'].notna().any()
    has_overviews = popular['overview'].notna().any()
    
    if has_posters and has_overviews:
        print("✓ TMDB integration is working!")
        print("✓ Posters and overviews are being fetched")
    elif api_key:
        print("⚠️  TMDB API key is set but data not fetching properly")
        print("   Check your API key or network connection")
    else:
        print("ℹ️  TMDB integration ready (set TMDB_API_KEY to enable)")
    
    print("\nTest complete!")
    print("=" * 60)


if __name__ == "__main__":
    test_tmdb_integration()
