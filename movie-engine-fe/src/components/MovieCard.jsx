import React, { useState } from 'react';
import MovieModal from './MovieModal';
import './MovieCard.css';

/**
 * Placeholder component for missing posters
 */
function PosterPlaceholder({ title }) {
  return (
    <div className="poster-placeholder">
      <svg viewBox="0 0 100 150" xmlns="http://www.w3.org/2000/svg">
        <rect width="100" height="150" fill="#16213e"/>
        <g opacity="0.3">
          <rect x="20" y="30" width="60" height="40" rx="2" fill="none" stroke="#ffffff" strokeWidth="2"/>
          <circle cx="35" cy="45" r="5" fill="#ffffff"/>
          <polygon points="20,70 35,55 50,60 65,50 80,70" fill="#ffffff"/>
        </g>
        <text x="50" y="100" textAnchor="middle" fill="#ffffff" fontSize="8" opacity="0.5">
          No Poster
        </text>
        <text x="50" y="110" textAnchor="middle" fill="#ffffff" fontSize="8" opacity="0.5">
          Available
        </text>
      </svg>
    </div>
  );
}

/**
 * MovieCard Component
 * Displays a movie with poster, title, year, and rating options
 */
export default function MovieCard({ movie, onRate, onSkip }) {
  const title = movie.title;
  const year = movie.year;
  const [imageError, setImageError] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const posterUrl = movie.poster_url;

  const handleRate = (rating) => {
    onRate(movie.movieId, rating);
  };

  const handleImageError = () => {
    setImageError(true);
  };

  const handlePosterClick = () => {
    setIsModalOpen(true);
  };

  return (
    <div className="movie-card">
      <div className="movie-poster-container" onClick={handlePosterClick} style={{ cursor: 'pointer' }}>
        {!posterUrl || imageError ? (
          <PosterPlaceholder title={title} />
        ) : (
          <img 
            src={posterUrl} 
            alt={title}
            className="movie-poster"
            onError={handleImageError}
          />
        )}
      </div>

      <MovieModal 
        movie={movie} 
        isOpen={isModalOpen} 
        onClose={() => setIsModalOpen(false)} 
      />

      <div className="movie-info">
        <h2 className="movie-title">{title}</h2>
        <p className="movie-year">{year}</p>
      </div>

      <div className="rating-buttons">
        <button 
          className="rating-btn rating-btn-dislike"
          onClick={() => handleRate(1.0)}
          aria-label="Dislike"
          title="Dislike (1 star)"
        >
          <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2"/>
            <circle cx="8" cy="9" r="1.5" fill="currentColor"/>
            <circle cx="16" cy="9" r="1.5" fill="currentColor"/>
            <path d="M8 16C8 16 9.5 14 12 14C14.5 14 16 16 16 16" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
          </svg>
        </button>

        <button 
          className="rating-btn rating-btn-neutral"
          onClick={() => handleRate(3.0)}
          aria-label="Neutral"
          title="Neutral (3 stars)"
        >
          <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2"/>
            <circle cx="8" cy="9" r="1.5" fill="currentColor"/>
            <circle cx="16" cy="9" r="1.5" fill="currentColor"/>
            <line x1="8" y1="15" x2="16" y2="15" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
          </svg>
        </button>

        <button 
          className="rating-btn rating-btn-like"
          onClick={() => handleRate(5.0)}
          aria-label="Like"
          title="Like (5 stars)"
        >
          <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2"/>
            <circle cx="8" cy="9" r="1.5" fill="currentColor"/>
            <circle cx="16" cy="9" r="1.5" fill="currentColor"/>
            <path d="M8 14C8 14 9.5 16 12 16C14.5 16 16 14 16 14" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
          </svg>
        </button>
      </div>

      <button 
        className="skip-btn"
        onClick={onSkip}
      >
        <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M7 4L17 12L7 20V4Z" fill="currentColor"/>
          <rect x="17" y="4" width="2" height="16" fill="currentColor"/>
        </svg>
      </button>
    </div>
  );
}
