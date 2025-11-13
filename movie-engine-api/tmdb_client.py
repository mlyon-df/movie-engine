"""
TMDB API Client Module
Handles fetching movie metadata (posters, overviews) from The Movie Database API.
"""

import requests
import logging
import os
from typing import Optional, Dict
from functools import lru_cache

logger = logging.getLogger(__name__)


class TMDBClient:
    """Client for interacting with The Movie Database (TMDB) API"""
    
    BASE_URL = "https://api.themoviedb.org/3"
    IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"  # w500 = 500px width
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize TMDB client.
        
        Parameters:
        - api_key: TMDB API key (if None, will check TMDB_API_KEY env var)
        """
        self.api_key = api_key or os.environ.get('TMDB_API_KEY')
        if not self.api_key:
            logger.warning("TMDB API key not provided. Set TMDB_API_KEY environment variable.")
        
        self.session = requests.Session()
        logger.info("TMDB client initialized")
    
    @lru_cache(maxsize=1000)
    def get_movie_details(self, tmdb_id: int) -> Optional[Dict]:
        """
        Fetch movie details from TMDB API.
        Results are cached to minimize API calls.
        
        Parameters:
        - tmdb_id: TMDB movie ID
        
        Returns:
        - Dict with movie details or None if failed
        """
        if not self.api_key:
            logger.warning("Cannot fetch TMDB data: API key not configured")
            return None
        
        if pd.isna(tmdb_id) or tmdb_id == 0:
            return None
        
        try:
            url = f"{self.BASE_URL}/movie/{int(tmdb_id)}"
            params = {"api_key": self.api_key}
            
            response = self.session.get(url, params=params, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract relevant fields
            result = {
                "poster_path": data.get("poster_path"),
                "overview": data.get("overview"),
                "poster_url": self._get_poster_url(data.get("poster_path")),
                "backdrop_path": data.get("backdrop_path"),
                "vote_average": data.get("vote_average"),
                "release_date": data.get("release_date")
            }
            
            return result
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching TMDB data for movie {tmdb_id}")
            return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"Movie {tmdb_id} not found in TMDB")
            else:
                logger.error(f"HTTP error fetching TMDB data for movie {tmdb_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching TMDB data for movie {tmdb_id}: {e}")
            return None
    
    def _get_poster_url(self, poster_path: Optional[str]) -> Optional[str]:
        """
        Convert TMDB poster path to full URL.
        
        Parameters:
        - poster_path: Path from TMDB API (e.g., "/abc123.jpg")
        
        Returns:
        - Full URL to poster image or None
        """
        if not poster_path:
            return None
        return f"{self.IMAGE_BASE_URL}{poster_path}"
    
    def enrich_movie_data(self, movie_id: int, tmdb_id: Optional[int]) -> Dict:
        """
        Enrich movie data with TMDB information.
        
        Parameters:
        - movie_id: MovieLens movie ID
        - tmdb_id: TMDB movie ID
        
        Returns:
        - Dict with poster_url and overview (None if unavailable)
        """
        if not tmdb_id or pd.isna(tmdb_id):
            return {
                "poster_url": None,
                "overview": None
            }
        
        details = self.get_movie_details(tmdb_id)
        
        if not details:
            return {
                "poster_url": None,
                "overview": None
            }
        
        return {
            "poster_url": details.get("poster_url"),
            "overview": details.get("overview")
        }


# Import pandas for isna check
import pandas as pd
