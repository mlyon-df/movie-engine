import React, { useEffect } from 'react';
import './MovieModal.css';

/**
 * MovieModal Component
 * Displays detailed movie information in a modal overlay
 */
export default function MovieModal({ movie, isOpen, onClose }) {
  // Handle ESC key to close modal
  useEffect(() => {
    const handleEscape = (e) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    if (isOpen) {
      document.addEventListener('keydown', handleEscape);
      // Prevent body scroll when modal is open
      document.body.style.overflow = 'hidden';
    }

    return () => {
      document.removeEventListener('keydown', handleEscape);
      document.body.style.overflow = 'unset';
    };
  }, [isOpen, onClose]);

  if (!isOpen || !movie) return null;

  // Handle backdrop click
  const handleBackdropClick = (e) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  };

  return (
    <div className="modal-backdrop" onClick={handleBackdropClick}>
      <div className="modal-content">
        <button className="modal-close" onClick={onClose} aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <line x1="18" y1="6" x2="6" y2="18" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
            <line x1="6" y1="6" x2="18" y2="18" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
          </svg>
        </button>

        <h2 className="modal-title">{movie.title}</h2>
        {movie.year && <p className="modal-year">{movie.year}</p>}

        {movie.genres && movie.genres.length > 0 && (
          <div className="modal-genres">
            {movie.genres.map((genre, index) => (
              <span key={index} className="genre-tag">{genre}</span>
            ))}
          </div>
        )}

        {movie.overview && (
          <div className="modal-overview">
            <h3>Overview</h3>
            <p>{movie.overview}</p>
          </div>
        )}

        {!movie.overview && (
          <p className="modal-no-overview">No overview available for this movie.</p>
        )}
      </div>
    </div>
  );
}
