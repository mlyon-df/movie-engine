const STORAGE_KEY = 'movie-engine-ratings';

/**
 * Get all user ratings from local storage
 * @returns {Object} Dictionary of movieId to rating
 */
export function getRatings() {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored ? JSON.parse(stored) : {};
  } catch (error) {
    console.error('Failed to load ratings from storage:', error);
    return {};
  }
}

/**
 * Save a single rating to local storage
 * @param {number} movieId - The movie ID
 * @param {number} rating - The rating value (1.0, 3.0, or 5.0)
 */
export function saveRating(movieId, rating) {
  try {
    const ratings = getRatings();
    ratings[movieId] = rating;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(ratings));
  } catch (error) {
    console.error('Failed to save rating to storage:', error);
    throw error;
  }
}

/**
 * Remove a single rating from local storage
 * @param {number} movieId - The movie ID to remove
 */
export function removeRating(movieId) {
  try {
    const ratings = getRatings();
    delete ratings[movieId];
    localStorage.setItem(STORAGE_KEY, JSON.stringify(ratings));
  } catch (error) {
    console.error('Failed to remove rating from storage:', error);
    throw error;
  }
}

/**
 * Clear all ratings from local storage
 */
export function clearRatings() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch (error) {
    console.error('Failed to clear ratings:', error);
    throw error;
  }
}

/**
 * Get count of ratings
 * @returns {number} Number of ratings stored
 */
export function getRatingCount() {
  const ratings = getRatings();
  return Object.keys(ratings).length;
}


/**
 * Store movie title and year by movieId
 * @param {number} movieId - The movie ID
 * @param {string} title - The movie title
 * @param {string} year - The movie year
 */
export function saveMovieInfo(movieId, title, year) {
  try {
    const movies = JSON.parse(localStorage.getItem('movie-engine-movies') || '{}');
    // Normalize title before storing
    movies[movieId] = { title, year };
    localStorage.setItem('movie-engine-movies', JSON.stringify(movies));
  } catch (error) {
    console.error('Failed to save movie info to storage:', error);
    throw error;
  }
}

/**
 * Get movie title and year by movieId
 * @param {number} movieId - The movie ID
 * @returns {Object|null} Movie info object or null if not found
 */
export function getMovieInfo(movieId) {
  try {
    const movies = JSON.parse(localStorage.getItem('movie-engine-movies') || '{}');
    return movies[movieId] || null;
  } catch (error) {
    console.error('Failed to get movie info from storage:', error);
    return null;
  }
}