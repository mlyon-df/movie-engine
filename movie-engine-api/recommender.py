"""
Recommendation Engine Module
Core recommendation logic using Item-Based Collaborative Filtering.
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class RecommendationEngine:
    """
    Item-Based Collaborative Filtering recommendation engine.
    Implements hybrid onboarding strategy for new users.
    """
    
    def __init__(self, item_similarity_df, movies_df, ratings_df):
        """
        Initialize the recommendation engine.
        
        Parameters:
        - item_similarity_df: Pre-computed item similarity matrix
        - movies_df: Movies dataframe with metadata
        - ratings_df: Historical ratings dataframe
        """
        self.item_similarity_df = item_similarity_df
        self.movies_df = movies_df
        self.ratings_df = ratings_df
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
        - DataFrame with popular movies
        """
        popular = self.ratings_df.groupby('movieId').agg({
            'rating': ['mean', 'count']
        }).reset_index()
        popular.columns = ['movieId', 'avg_rating', 'rating_count']
        
        # Filter for movies with at least 50 ratings
        popular = popular[popular['rating_count'] >= 50]
        popular = popular.sort_values('avg_rating', ascending=False).head(n)
        
        result = self.movies_df[self.movies_df['movieId'].isin(popular['movieId'])][['movieId', 'title']].merge(
            popular[['movieId', 'avg_rating']], on='movieId'
        ).rename(columns={'avg_rating': 'predicted_rating'})
        
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
            personalized = self.movies_df[['movieId', 'title']].merge(personalized, on='movieId')
            
            # Get popular recommendations
            popular = self.get_popular_movies(n * 2)
            
            # Combine and deduplicate
            combined = pd.concat([personalized, popular]).drop_duplicates('movieId')
            return combined.head(n)
        
        # Case 3: Sufficient ratings (5+) - fully personalized
        logger.info(f"{num_ratings} ratings provided. Generating fully personalized recommendations.")
        predictions_df = predictions_df.sort_values('predicted_rating', ascending=False).head(n)
        recommendations = self.movies_df[['movieId', 'title']].merge(predictions_df, on='movieId')
        
        return recommendations
    
    def find_similar_movies(self, movie_id, n=10):
        """
        Find movies similar to a given movie.
        
        Parameters:
        - movie_id: ID of the movie
        - n: Number of similar movies to return
        
        Returns:
        - DataFrame with similar movies and similarity scores
        """
        if movie_id not in self.item_similarity_df.columns:
            logger.warning(f"Movie {movie_id} not found in similarity matrix")
            return pd.DataFrame(columns=['movieId', 'title', 'similarity'])
        
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
        
        return pd.DataFrame(results)
