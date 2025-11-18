import React, { useState } from 'react';
import ConfirmationModal from './ConfirmationModal';
import { getMovieInfo } from '../services/storage';
import './Drawer.css';

/**
 * Get rating label (Dislike, Neutral, Like)
 */
function getRatingLabel(rating) {
  if (rating <= 1.5) return 'Dislike';
  if (rating <= 3.5) return 'Neutral';
  return 'Like';
}

/**
 * Get rating emoji
 */
function getRatingIcon(rating) {
  if (rating <= 1.5) {
    // Frown face
    return (
      <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" className="rating-icon">
        <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2" fill="currentColor"/>
        <circle cx="8" cy="9" r="1.5" fill="#1a1a2e"/>
        <circle cx="16" cy="9" r="1.5" fill="#1a1a2e"/>
        <path d="M8 16C8 16 9.5 14 12 14C14.5 14 16 16 16 16" stroke="#1a1a2e" strokeWidth="2" strokeLinecap="round"/>
      </svg>
    );
  }
  if (rating <= 3.5) {
    // Neutral face
    return (
      <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" className="rating-icon">
        <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2" fill="currentColor"/>
        <circle cx="8" cy="9" r="1.5" fill="#1a1a2e"/>
        <circle cx="16" cy="9" r="1.5" fill="#1a1a2e"/>
        <line x1="8" y1="15" x2="16" y2="15" stroke="#1a1a2e" strokeWidth="2" strokeLinecap="round"/>
      </svg>
    );
  }
  // Smile face
  return (
    <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" className="rating-icon">
      <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2" fill="currentColor"/>
      <circle cx="8" cy="9" r="1.5" fill="#1a1a2e"/>
      <circle cx="16" cy="9" r="1.5" fill="#1a1a2e"/>
      <path d="M8 14C8 14 9.5 16 12 16C14.5 16 16 14 16 14" stroke="#1a1a2e" strokeWidth="2" strokeLinecap="round"/>
    </svg>
  );
}

/**
 * Get rating class name
 */
function getRatingClassName(rating) {
  if (rating <= 1.5) return 'rating-disliked';
  if (rating <= 3.5) return 'rating-neutral';
  return 'rating-liked';
}

export default function Drawer({ isOpen, onClose, ratings, onRemoveRating, onClearAll }) {
  const [modalState, setModalState] = useState({
    isOpen: false,
    type: null, // 'single' or 'all'
    movieId: null,
    movieTitle: null
  });

  const handleRemoveSingle = (movieId, movieTitle) => {
    setModalState({
      isOpen: true,
      type: 'single',
      movieId,
      movieTitle
    });
  };

  const handleRemoveAll = () => {
    setModalState({
      isOpen: true,
      type: 'all',
      movieId: null,
      movieTitle: null
    });
  };

  const handleConfirmRemove = () => {
    if (modalState.type === 'single') {
      onRemoveRating(modalState.movieId);
    } else if (modalState.type === 'all') {
      onClearAll();
    }
    setModalState({ isOpen: false, type: null, movieId: null, movieTitle: null });
  };

  const handleCancelRemove = () => {
    setModalState({ isOpen: false, type: null, movieId: null, movieTitle: null });
  };

  // Convert ratings object to array with movie details
  const ratingsList = Object.entries(ratings).map(([movieId, rating]) => {
    const movieInfo = getMovieInfo(parseInt(movieId));
    
    return {
      movieId: parseInt(movieId),
      rating: rating,
      title: movieInfo.title || `Movie ${movieId}`,
      year: movieInfo.year || 'Unknown',
      hasDetails: !!movieInfo
    };

  }).sort((a, b) => {
    // Sort by title
    const titleA = a.title.toLowerCase();
    const titleB = b.title.toLowerCase();
    return titleA.localeCompare(titleB);
  });

  return (
    <>
      <div 
        className={`drawer-overlay ${isOpen ? 'drawer-overlay-open' : ''}`}
        onClick={onClose}
      />
      
      <div className={`drawer ${isOpen ? 'drawer-open' : ''}`}>
        <div className="drawer-header">
          <h2 className="drawer-title">My Ratings ({ratingsList.length})</h2>
          <button className="drawer-close-btn" onClick={onClose} aria-label="Close drawer">
            <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <line x1="18" y1="6" x2="6" y2="18" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
              <line x1="6" y1="6" x2="18" y2="18" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
            </svg>
          </button>
        </div>

        <div className="drawer-content">
          {ratingsList.length === 0 ? (
            <div className="drawer-empty">
              <p>No ratings yet</p>
              <p className="drawer-empty-hint">Start rating movies to see them here!</p>
            </div>
          ) : (
            <ul className="ratings-list">
              {ratingsList.map(({ movieId, rating, title, year }) => (
                <li key={movieId} className="rating-item">
                  <div className="rating-item-info">
                    <div className="rating-item-title">
                      {title}
                    </div>
                    <div className="rating-item-meta">
                      <span className="rating-item-year">{year}</span>
                      <span className="rating-item-separator">•</span>
                      <span className={`rating-item-rating ${getRatingClassName(rating)}`}>
                        {getRatingIcon(rating)} {getRatingLabel(rating)}
                      </span>
                    </div>
                  </div>
                  <button
                    className="rating-item-remove"
                    onClick={() => handleRemoveSingle(movieId, title)}
                    aria-label={`Remove rating for ${title}`}
                  >
                    <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                      <path d="M3 6h18M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                      <line x1="10" y1="11" x2="10" y2="17" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
                      <line x1="14" y1="11" x2="14" y2="17" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
                    </svg>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        {ratingsList.length > 0 && (
          <div className="drawer-footer">
            <button className="clear-all-btn" onClick={handleRemoveAll}>
              Clear All Ratings
            </button>
          </div>
        )}
      </div>

      <ConfirmationModal
        isOpen={modalState.isOpen}
        title={modalState.type === 'single' ? 'Remove Rating?' : 'Clear All Ratings?'}
        message={
          modalState.type === 'single'
            ? `Are you sure you want to remove your rating for "${modalState.movieTitle}"?`
            : `Are you sure you want to remove all ${ratingsList.length} rating${ratingsList.length === 1 ? '' : 's'}?`
        }
        onConfirm={handleConfirmRemove}
        onCancel={handleCancelRemove}
        confirmText="Remove"
        cancelText="Cancel"
      />
    </>
  );
}
