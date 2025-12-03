"""
Recommendation Engine Module
Core recommendation logic using Item-Based Collaborative Filtering.
"""

import pandas as pd
import numpy as np
import logging
from tmdb_client import TMDBClient

logger = logging.getLogger(__name__)


class RecommendationEngine:
    """
    Item-Based Collaborative Filtering recommendation engine.
    Implements hybrid onboarding strategy for new users.
    """
    
    def __init__(self, item_similarity_df, movies_df, ratings_df, links_df=None, tmdb_api_key=None):
        """
        Initialize the recommendation engine.
        
        Parameters:
        - item_similarity_df: Pre-computed item similarity matrix
        - movies_df: Movies dataframe with metadata
        - ratings_df: Historical ratings dataframe
        - links_df: Links dataframe with TMDB IDs (optional)
        - tmdb_api_key: TMDB API key (optional, can be set via env var)
        """
        self.item_similarity_df = item_similarity_df
        self.ratings_df = ratings_df
        self.links_df = links_df
        
        # Identify genre columns (one-hot encoded)
        # Genre columns are those that are not movieId or title
        self.genre_columns = [col for col in movies_df.columns 
                             if col not in ['movieId', 'title'] and movies_df[col].dtype in ['int64', 'int32', 'float64']]
        
        # Create a genres list column from one-hot encoded columns (cached in memory)
        if self.genre_columns:
            logger.info(f"Found {len(self.genre_columns)} genre columns. Building genres cache...")
            # Pre-compute genres as a list for each movie - this is done once and cached
            movies_df = movies_df.copy()
            movies_df['genres'] = movies_df[self.genre_columns].apply(
                lambda row: [self.genre_columns[i] for i, val in enumerate(row) if val == 1],
                axis=1
            )
            logger.info("Genres cache built successfully")
        
        # Store the movies_df with genres column
        self.movies_df = movies_df
        
        # Initialize TMDB client if links are provided
        self.tmdb_client = None
        if links_df is not None:
            self.tmdb_client = TMDBClient(api_key=tmdb_api_key)
            logger.info("TMDB client initialized")
        
        logger.info("Recommendation engine initialized")
    
    def predict_rating(self, movie_id, user_ratings, k=10):
        """
        Predict rating for a specific movie based on user's ratings.
        
        Parameters:
        - movie_id: ID of movie to predict rating for
        - user_ratings: Dict of {movieId: rating} for movies the user has rated
        - k: Number of similar items to consider
        
        Returns:
        - Predicted rating (float)
        """
        if movie_id not in self.item_similarity_df.columns:
            return 3.0  # Default neutral rating
        
        # Get similarity to movies the user has rated
        similarities = self.item_similarity_df[movie_id]
        
        weighted_sum = 0
        similarity_sum = 0
        
        # Calculate weighted average based on similar movies user has rated
        for rated_movie_id, rating in user_ratings.items():
            if rated_movie_id in similarities.index:
                sim = similarities[rated_movie_id]
                weighted_sum += sim * rating
                similarity_sum += abs(sim)
        
        if similarity_sum == 0:
            return 3.0
        
        predicted_rating = weighted_sum / similarity_sum
        return np.clip(predicted_rating, 0.5, 5.0)
    
    def get_popular_movies(self, n=10):
        """
        Get popular movies based on average rating and rating count.
        
        Parameters:
        - n: Number of movies to return
        
        Returns:
        - DataFrame with popular movies (including TMDB data)
        """
        popular = self.ratings_df.groupby('movieId').agg({
            'rating': ['mean', 'count']
        }).reset_index()
        popular.columns = ['movieId', 'avg_rating', 'rating_count']
        
        # Filter for movies with at least 50 ratings
        popular = popular[popular['rating_count'] >= 50]
        popular = popular.sort_values('avg_rating', ascending=False).head(n)
        
        # Include genres if available
        movie_cols = ['movieId', 'title']
        if 'genres' in self.movies_df.columns:
            movie_cols.append('genres')
        
        result = self.movies_df[self.movies_df['movieId'].isin(popular['movieId'])][movie_cols].merge(
            popular[['movieId', 'avg_rating']], on='movieId'
        ).rename(columns={'avg_rating': 'predicted_rating'})
        
        # Enrich with TMDB data
        result = self._enrich_with_tmdb(result)
        
        return result
    
    def recommend(self, user_ratings, n=10, k=10):
        """
        Generate movie recommendations using hybrid onboarding strategy.
        
        Strategy:
        - No ratings: Return popular movies
        - Few ratings (1-4): Blend personalized + popular recommendations
        - Sufficient ratings (5+): Fully personalized item-based CF
        
        Parameters:
        - user_ratings: Dict of {movieId: rating} for movies the user has rated
        - n: Number of recommendations to return
        - k: Number of similar items to consider in predictions
        
        Returns:
        - DataFrame with columns: movieId, title, predicted_rating
        """
        num_ratings = len(user_ratings)
        
        # Case 1: No ratings - recommend popular movies
        if num_ratings == 0:
            logger.info("No ratings provided. Returning popular movies.")
            return self.get_popular_movies(n)
        
        # Get all available movies
        rated_movie_ids = list(user_ratings.keys())
        all_movie_ids = self.item_similarity_df.columns.tolist()
        unrated_movies = [m for m in all_movie_ids if m not in rated_movie_ids]
        
        # Generate personalized predictions
        predictions = []
        for movie_id in unrated_movies:
            predicted_rating = self.predict_rating(movie_id, user_ratings, k)
            predictions.append({
                'movieId': movie_id,
                'predicted_rating': predicted_rating
            })
        
        predictions_df = pd.DataFrame(predictions)
        
        if len(predictions_df) == 0:
            return self.get_popular_movies(n)
        
        # Case 2: Few ratings (1-4) - blend personalized + popular
        if num_ratings < 5:
            logger.info(f"{num_ratings} rating(s) provided. Blending personalized and popular recommendations.")
            
            # Get top personalized recommendations
            personalized = predictions_df.sort_values('predicted_rating', ascending=False).head(n * 2)
            # Include genres if available
            movie_cols = ['movieId', 'title']
            if 'genres' in self.movies_df.columns:
                movie_cols.append('genres')
            
            personalized = self.movies_df[movie_cols].merge(personalized, on='movieId')
            
            # Get popular recommendations
            popular = self.get_popular_movies(n * 2)
            
            # Combine and deduplicate
            combined = pd.concat([personalized, popular]).drop_duplicates('movieId')
            
            # Enrich with TMDB data
            combined = self._enrich_with_tmdb(combined)
            
            return combined.head(n)
        
        # Case 3: Sufficient ratings (5+) - fully personalized
        logger.info(f"{num_ratings} ratings provided. Generating fully personalized recommendations.")
        predictions_df = predictions_df.sort_values('predicted_rating', ascending=False).head(n)
        
        # Include genres if available
        movie_cols = ['movieId', 'title']
        if 'genres' in self.movies_df.columns:
            movie_cols.append('genres')
        
        recommendations = self.movies_df[movie_cols].merge(predictions_df, on='movieId')
        
        # Enrich with TMDB data
        recommendations = self._enrich_with_tmdb(recommendations)
        
        return recommendations
    
    def find_similar_movies(self, movie_id, n=10):
        """
        Find movies similar to a given movie.
        
        Parameters:
        - movie_id: ID of the movie
        - n: Number of similar movies to return
        
        Returns:
        - DataFrame with similar movies, similarity scores, and TMDB data
        """
        if movie_id not in self.item_similarity_df.columns:
            logger.warning(f"Movie {movie_id} not found in similarity matrix")
            return pd.DataFrame(columns=['movieId', 'title', 'similarity', 'poster_url', 'overview'])
        
        # Get similar movies
        similar_movies = self.item_similarity_df[movie_id].sort_values(ascending=False)[1:n+1]
        
        # Create results dataframe
        results = []
        for sim_movie_id, similarity in similar_movies.items():
            movie_info = self.movies_df[self.movies_df['movieId'] == sim_movie_id]
            if len(movie_info) > 0:
                results.append({
                    'movieId': sim_movie_id,
                    'title': movie_info.iloc[0]['title'],
                    'similarity': similarity
                })
        
        results_df = pd.DataFrame(results)
        
        # Enrich with TMDB data
        if len(results_df) > 0:
            results_df = self._enrich_with_tmdb(results_df)
        else:
            results_df['poster_url'] = None
            results_df['overview'] = None
        
        return results_df
    
    def _enrich_with_tmdb(self, recommendations_df):
        """
        Enrich recommendations with TMDB data (poster URL and overview).
        
        Parameters:
        - recommendations_df: DataFrame with movieId, title, and predicted_rating
        
        Returns:
        - Enhanced DataFrame with poster_url and overview columns
        """
        if self.tmdb_client is None or self.links_df is None:
            # If TMDB client not available, add None columns
            recommendations_df['poster_url'] = None
            recommendations_df['overview'] = None
            return recommendations_df
        
        # Merge with links to get TMDB IDs
        enriched = recommendations_df.merge(
            self.links_df[['movieId', 'tmdbId']], 
            on='movieId', 
            how='left'
        )
        
        # Fetch TMDB data for each movie
        tmdb_data = []
        for _, row in enriched.iterrows():
            tmdb_id = row.get('tmdbId')
            tmdb_info = self.tmdb_client.enrich_movie_data(row['movieId'], tmdb_id)
            tmdb_data.append(tmdb_info)
        
        # Add TMDB columns
        enriched['poster_url'] = [d['poster_url'] for d in tmdb_data]
        enriched['overview'] = [d['overview'] for d in tmdb_data]
        
        # Drop tmdbId column (internal use only)
        enriched = enriched.drop(columns=['tmdbId'], errors='ignore')
        
        return enriched
