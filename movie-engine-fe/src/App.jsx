import React, { useState, useEffect } from 'react';
import MovieCard from './components/MovieCard';
import Drawer from './components/Drawer';
import { getRecommendations } from './services/api';
import { getRatings, saveRating, clearRatings, removeRating } from './services/storage';
import './App.css';

function App() {
  const [recommendations, setRecommendations] = useState([]);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [moviesData, setMoviesData] = useState({});

  // Load initial recommendations
  useEffect(() => {
    loadRecommendations();
  }, []);

  const loadRecommendations = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const userRatings = getRatings();
      const response = await getRecommendations(userRatings, 10);
      
      if (response.recommendations && response.recommendations.length > 0) {
        // Randomize the order
        const shuffled = [...response.recommendations].sort(() => Math.random() - 0.5);
        setRecommendations(shuffled);
        setCurrentIndex(0);
        
        // Store movie data for the drawer
        const newMoviesData = { ...moviesData };
        response.recommendations.forEach(movie => {
          newMoviesData[movie.movieId] = {
            title: movie.title,
            poster_url: movie.poster_url,
            overview: movie.overview
          };
        });
        setMoviesData(newMoviesData);
      } else {
        setError('No recommendations available. Please try again.');
      }
    } catch (err) {
      console.error('Failed to load recommendations:', err);
      setError('Failed to load recommendations. Please check your connection and try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleRate = async (movieId, rating) => {
    try {
      // Save rating to local storage
      saveRating(movieId, rating);

      // Refresh recommendations with updated ratings
      setIsRefreshing(true);
      const userRatings = getRatings();
      const response = await getRecommendations(userRatings, 10);
      
      if (response.recommendations && response.recommendations.length > 0) {
        // Store movie data
        const newMoviesData = { ...moviesData };
        response.recommendations.forEach(movie => {
          newMoviesData[movie.movieId] = {
            title: movie.title,
            poster_url: movie.poster_url,
            overview: movie.overview
          };
        });
        setMoviesData(newMoviesData);
        
        // Filter out already rated movies
        const ratedMovieIds = Object.keys(userRatings).map(Number);
        const unratedMovies = response.recommendations.filter(
          movie => !ratedMovieIds.includes(movie.movieId)
        );
        
        if (unratedMovies.length > 0) {
          // Randomize the order
          const shuffled = [...unratedMovies].sort(() => Math.random() - 0.5);
          setRecommendations(shuffled);
          setCurrentIndex(0);
        } else {
          // If all recommended movies are rated, get new recommendations
          await loadRecommendations();
        }
      }
    } catch (err) {
      console.error('Failed to update recommendations:', err);
      // Even if fetching new recommendations fails, move to next movie
      handleSkip();
    } finally {
      setIsRefreshing(false);
    }
  };

  const handleSkip = () => {
    if (currentIndex < recommendations.length - 1) {
      setCurrentIndex(currentIndex + 1);
    } else {
      // If we've gone through all recommendations, reload
      loadRecommendations();
    }
  };

  const handleClearRatings = () => {
    clearRatings();
    setIsDrawerOpen(false);
    loadRecommendations();
  };

  const handleRemoveRating = (movieId) => {
    removeRating(movieId);
    // Optionally reload recommendations when a rating is removed
    loadRecommendations();
  };

  const handleToggleDrawer = () => {
    setIsDrawerOpen(!isDrawerOpen);
  };

  const currentMovie = recommendations[currentIndex];

  return (
    <div className="app">
      <header className="app-header">
        <button className="menu-btn" aria-label="Menu" onClick={handleToggleDrawer}>
          <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <line x1="3" y1="6" x2="21" y2="6" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
            <line x1="3" y1="12" x2="21" y2="12" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
            <line x1="3" y1="18" x2="21" y2="18" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
          </svg>
        </button>
        <h1 className="app-title">Movie Recommender</h1>
      </header>

      <Drawer
        isOpen={isDrawerOpen}
        onClose={() => setIsDrawerOpen(false)}
        ratings={getRatings()}
        moviesData={moviesData}
        onRemoveRating={handleRemoveRating}
        onClearAll={handleClearRatings}
      />

      <main className="app-main">
        {loading && (
          <div className="loading-container">
            <div className="spinner"></div>
            <p>Loading recommendations...</p>
          </div>
        )}

        {error && (
          <div className="error-container">
            <p className="error-message">{error}</p>
            <button className="retry-btn" onClick={loadRecommendations}>
              Retry
            </button>
          </div>
        )}

        {!loading && !error && currentMovie && (
          <>
            {isRefreshing && (
              <div className="refreshing-overlay">
                <div className="spinner"></div>
              </div>
            )}
            <MovieCard
              movie={currentMovie}
              onRate={handleRate}
              onSkip={handleSkip}
            />
          </>
        )}
      </main>
    </div>
  );
}

export default App;
