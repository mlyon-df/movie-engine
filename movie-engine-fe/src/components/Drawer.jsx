import React, { useState } from 'react';
import ConfirmationModal from './ConfirmationModal';
import './Drawer.css';

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
function getRatingEmoji(rating) {
  if (rating <= 1.5) return '😞';
  if (rating <= 3.5) return '😐';
  return '😊';
}

export default function Drawer({ isOpen, onClose, ratings, moviesData, onRemoveRating, onClearAll }) {
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
    const movie = moviesData[movieId];
    return {
      movieId: parseInt(movieId),
      rating: rating,
      title: movie ? movie.title : `Movie ${movieId}`,
      hasDetails: !!movie
    };
  }).sort((a, b) => {
    // Sort by title
    const titleA = extractTitle(a.title).toLowerCase();
    const titleB = extractTitle(b.title).toLowerCase();
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
              {ratingsList.map(({ movieId, rating, title }) => (
                <li key={movieId} className="rating-item">
                  <div className="rating-item-info">
                    <div className="rating-item-title">
                      {extractTitle(title)}
                    </div>
                    <div className="rating-item-meta">
                      <span className="rating-item-year">{extractYear(title)}</span>
                      <span className="rating-item-separator">•</span>
                      <span className="rating-item-rating">
                        {getRatingEmoji(rating)} {getRatingLabel(rating)}
                      </span>
                    </div>
                  </div>
                  <button
                    className="rating-item-remove"
                    onClick={() => handleRemoveSingle(movieId, extractTitle(title))}
                    aria-label={`Remove rating for ${extractTitle(title)}`}
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
