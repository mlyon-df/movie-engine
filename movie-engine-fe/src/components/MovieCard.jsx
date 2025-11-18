import React from 'react';
import './MovieCard.css';

/**
 * Extract year from title in format "Movie Name (YYYY)"
 */
function extractYear(title) {
  const match = title.match(/\((\d{4})\)$/);
  return match ? match[1] : '';
}

/**
 * Remove year from title
 */
function extractTitle(title) {
  return title.replace(/\s*\(\d{4}\)\s*$/, '').trim();
}

/**
 * MovieCard Component
 * Displays a movie with poster, title, year, and rating options
 */
export default function MovieCard({ movie, onRate, onSkip }) {
  const title = extractTitle(movie.title);
  const year = extractYear(movie.title);
  const posterUrl = movie.poster_url || 'https://via.placeholder.com/500x750/1a1a2e/ffffff?text=No+Poster';

  const handleRate = (rating) => {
    onRate(movie.movieId, rating);
  };

  return (
    <div className="movie-card">
      <div className="movie-poster-container">
        <img 
          src={posterUrl} 
          alt={title}
          className="movie-poster"
          onError={(e) => {
            e.target.src = 'https://via.placeholder.com/500x750/1a1a2e/ffffff?text=No+Poster';
          }}
        />
      </div>

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
