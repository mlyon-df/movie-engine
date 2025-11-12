# Updated model_loader.py with S3 support
"""
Model Loader Module
Handles loading and caching of the pre-trained ML model and data.
Supports both local file system and S3 bucket.
"""

import pickle
import json
import pandas as pd
from pathlib import Path
import logging
import os
import io

logger = logging.getLogger(__name__)


class ModelLoader:
    """Loads and caches the recommendation model and data from local or S3"""
    
    def __init__(self, model_dir=None, data_dir=None, s3_bucket=None):
        """
        Initialize the model loader.
        
        Parameters:
        - model_dir: Path to model files (default: ../movie-engine-data/models)
        - data_dir: Path to processed data (default: ../movie-engine-data/processed/ml-100k)
        - s3_bucket: S3 bucket name (if None, will check AWS_LAMBDA_FUNCTION_NAME env var)
        """
        # Detect if running in Lambda
        self.is_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
        
        # S3 bucket configuration
        if s3_bucket is None:
            s3_bucket = os.environ.get('S3_BUCKET_NAME', 'movie-engine-data')
        self.s3_bucket = s3_bucket
        
        # Initialize S3 client if in Lambda or S3 bucket specified
        self.s3_client = None
        if self.is_lambda or s3_bucket != 'movie-engine-data':
            try:
                import boto3
                self.s3_client = boto3.client('s3')
                logger.info(f"S3 client initialized for bucket: {self.s3_bucket}")
            except ImportError:
                logger.warning("boto3 not available. Install with: pip install boto3")
                if self.is_lambda:
                    raise RuntimeError("boto3 is required for Lambda deployment")
        
        # Set default local paths
        if model_dir is None:
            model_dir = Path(__file__).parent.parent / 'movie-engine-data' / 'models'
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'movie-engine-data' / 'processed' / 'ml-100k'
        
        self.model_dir = Path(model_dir)
        self.data_dir = Path(data_dir)
        
        # Load all components
        logger.info(f"Loading model and data (Lambda: {self.is_lambda}, S3: {self.s3_client is not None})...")
        self._load_model()
        self._load_data()
        logger.info("Model and data loaded successfully!")
    
    def _load_from_s3(self, s3_key):
        """Load a file from S3"""
        logger.info(f"Loading from S3: s3://{self.s3_bucket}/{s3_key}")
        response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
        return response['Body'].read()
    
    def _load_pickle_file(self, local_path, s3_key):
        """Load a pickle file from local or S3"""
        if self.s3_client and self.is_lambda:
            # Load from S3
            data = self._load_from_s3(s3_key)
            return pickle.loads(data)
        else:
            # Load from local file
            logger.info(f"Loading from local file: {local_path}")
            with open(local_path, 'rb') as f:
                return pickle.load(f)
    
    def _load_json_file(self, local_path, s3_key):
        """Load a JSON file from local or S3"""
        if self.s3_client and self.is_lambda:
            # Load from S3
            data = self._load_from_s3(s3_key)
            return json.loads(data.decode('utf-8'))
        else:
            # Load from local file
            logger.info(f"Loading from local file: {local_path}")
            with open(local_path, 'r') as f:
                return json.load(f)
    
    def _load_csv_file(self, local_path, s3_key):
        """Load a CSV file from local or S3"""
        if self.s3_client and self.is_lambda:
            # Load from S3
            data = self._load_from_s3(s3_key)
            return pd.read_csv(io.BytesIO(data))
        else:
            # Load from local file
            logger.info(f"Loading from local file: {local_path}")
            return pd.read_csv(local_path)
    
    def _load_model(self):
        """Load the pre-trained model files"""
        try:
            # Load item similarity matrix
            similarity_local = self.model_dir / 'item_similarity_matrix.pkl'
            similarity_s3 = 'models/item_similarity_matrix.pkl'
            logger.info("Loading item similarity matrix...")
            self.item_similarity_df = self._load_pickle_file(similarity_local, similarity_s3)
            
            # Load user-item matrix (optional, for reference)
            user_item_local = self.model_dir / 'user_item_matrix.pkl'
            user_item_s3 = 'models/user_item_matrix.pkl'
            logger.info("Loading user-item matrix...")
            self.user_item_matrix = self._load_pickle_file(user_item_local, user_item_s3)
            
            # Load model metadata
            metadata_local = self.model_dir / 'item_based_metadata.json'
            metadata_s3 = 'models/item_based_metadata.json'
            logger.info("Loading model metadata...")
            self.metadata = self._load_json_file(metadata_local, metadata_s3)
            
            logger.info(f"Model loaded - Type: {self.metadata['model_type']}, "
                       f"RMSE: {self.metadata['rmse']}, MAE: {self.metadata['mae']}")
            
        except Exception as e:
            logger.error(f"Error loading model: {e}", exc_info=True)
            raise RuntimeError(f"Failed to load model: {e}")
    
    def _load_data(self):
        """Load the movie and ratings data"""
        try:
            # Load movies
            movies_local = self.data_dir / 'movies.csv'
            movies_s3 = 'processed/ml-100k/movies.csv'
            logger.info("Loading movies...")
            self.movies_df = self._load_csv_file(movies_local, movies_s3)
            
            # Load ratings
            ratings_local = self.data_dir / 'ratings.csv'
            ratings_s3 = 'processed/ml-100k/ratings.csv'
            logger.info("Loading ratings...")
            self.ratings_df = self._load_csv_file(ratings_local, ratings_s3)
            
            logger.info(f"Data loaded - {len(self.movies_df)} movies, {len(self.ratings_df)} ratings")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}", exc_info=True)
            raise RuntimeError(f"Failed to load data: {e}")
    
    def get_model_info(self):
        """Get information about the loaded model"""
        return {
            'model_type': self.metadata['model_type'],
            'dataset': self.metadata['dataset'],
            'created_date': self.metadata['created_date'],
            'rmse': self.metadata['rmse'],
            'mae': self.metadata['mae'],
            'num_users': self.metadata['num_users'],
            'num_movies': self.metadata['num_movies'],
            'k_neighbors': self.metadata['k_neighbors'],
            'source': 's3' if (self.s3_client and self.is_lambda) else 'local'
        }

import pickle
import json
import pandas as pd
from pathlib import Path
import logging
import os
import io

logger = logging.getLogger(__name__)


class ModelLoader:
    """Loads and caches the recommendation model and data from local or S3"""
    
    def __init__(self, model_dir=None, data_dir=None, s3_bucket=None):
        """
        Initialize the model loader.
        
        Parameters:
        - model_dir: Path to model files (default: ../movie-engine-data/models)
        - data_dir: Path to processed data (default: ../movie-engine-data/processed/ml-100k)
        - s3_bucket: S3 bucket name (if None, will check AWS_LAMBDA_FUNCTION_NAME env var)
        """
        # Detect if running in Lambda
        self.is_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
        
        # S3 bucket configuration
        if s3_bucket is None:
            s3_bucket = os.environ.get('S3_BUCKET_NAME', 'movie-engine-data')
        self.s3_bucket = s3_bucket
        
        # Initialize S3 client if in Lambda or S3 bucket specified
        self.s3_client = None
        if self.is_lambda or s3_bucket != 'movie-engine-data':
            try:
                import boto3
                self.s3_client = boto3.client('s3')
                logger.info(f"S3 client initialized for bucket: {self.s3_bucket}")
            except ImportError:
                logger.warning("boto3 not available. Install with: pip install boto3")
                if self.is_lambda:
                    raise RuntimeError("boto3 is required for Lambda deployment")
        
        # Set default local paths
        if model_dir is None:
            model_dir = Path(__file__).parent.parent / 'movie-engine-data' / 'models'
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'movie-engine-data' / 'processed' / 'ml-100k'
        
        self.model_dir = Path(model_dir)
        self.data_dir = Path(data_dir)
        
        # Load all components
        logger.info(f"Loading model and data (Lambda: {self.is_lambda}, S3: {self.s3_client is not None})...")
        self._load_model()
        self._load_data()
        logger.info("Model and data loaded successfully!")
    
    def _load_from_s3(self, s3_key):
        """Load a file from S3"""
        logger.info(f"Loading from S3: s3://{self.s3_bucket}/{s3_key}")
        response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
        return response['Body'].read()
    
    def _load_pickle_file(self, local_path, s3_key):
        """Load a pickle file from local or S3"""
        if self.s3_client and self.is_lambda:
            # Load from S3
            data = self._load_from_s3(s3_key)
            return pickle.loads(data)
        else:
            # Load from local file
            logger.info(f"Loading from local file: {local_path}")
            with open(local_path, 'rb') as f:
                return pickle.load(f)
    
    def _load_json_file(self, local_path, s3_key):
        """Load a JSON file from local or S3"""
        if self.s3_client and self.is_lambda:
            # Load from S3
            data = self._load_from_s3(s3_key)
            return json.loads(data.decode('utf-8'))
        else:
            # Load from local file
            logger.info(f"Loading from local file: {local_path}")
            with open(local_path, 'r') as f:
                return json.load(f)
    
    def _load_csv_file(self, local_path, s3_key):
        """Load a CSV file from local or S3"""
        if self.s3_client and self.is_lambda:
            # Load from S3
            data = self._load_from_s3(s3_key)
            return pd.read_csv(io.BytesIO(data))
        else:
            # Load from local file
            logger.info(f"Loading from local file: {local_path}")
            return pd.read_csv(local_path)
    
    def _load_model(self):
        """Load the pre-trained model files"""
        try:
            # Load item similarity matrix
            similarity_local = self.model_dir / 'item_similarity_matrix.pkl'
            similarity_s3 = 'models/item_similarity_matrix.pkl'
            logger.info("Loading item similarity matrix...")
            self.item_similarity_df = self._load_pickle_file(similarity_local, similarity_s3)
            
            # Load user-item matrix (optional, for reference)
            user_item_local = self.model_dir / 'user_item_matrix.pkl'
            user_item_s3 = 'models/user_item_matrix.pkl'
            logger.info("Loading user-item matrix...")
            self.user_item_matrix = self._load_pickle_file(user_item_local, user_item_s3)
            
            # Load model metadata
            metadata_local = self.model_dir / 'item_based_metadata.json'
            metadata_s3 = 'models/item_based_metadata.json'
            logger.info("Loading model metadata...")
            self.metadata = self._load_json_file(metadata_local, metadata_s3)
            
            logger.info(f"Model loaded - Type: {self.metadata['model_type']}, "
                       f"RMSE: {self.metadata['rmse']}, MAE: {self.metadata['mae']}")
            
        except Exception as e:
            logger.error(f"Error loading model: {e}", exc_info=True)
            raise RuntimeError(f"Failed to load model: {e}")
    
    def _load_data(self):
        """Load the movie and ratings data"""
        try:
            # Load movies
            movies_local = self.data_dir / 'movies.csv'
            movies_s3 = 'processed/ml-100k/movies.csv'
            logger.info("Loading movies...")
            self.movies_df = self._load_csv_file(movies_local, movies_s3)
            
            # Load ratings
            ratings_local = self.data_dir / 'ratings.csv'
            ratings_s3 = 'processed/ml-100k/ratings.csv'
            logger.info("Loading ratings...")
            self.ratings_df = self._load_csv_file(ratings_local, ratings_s3)
            
            logger.info(f"Data loaded - {len(self.movies_df)} movies, {len(self.ratings_df)} ratings")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}", exc_info=True)
            raise RuntimeError(f"Failed to load data: {e}")
    
    def get_model_info(self):
        """Get information about the loaded model"""
        return {
            'model_type': self.metadata['model_type'],
            'dataset': self.metadata['dataset'],
            'created_date': self.metadata['created_date'],
            'rmse': self.metadata['rmse'],
            'mae': self.metadata['mae'],
            'num_users': self.metadata['num_users'],
            'num_movies': self.metadata['num_movies'],
            'k_neighbors': self.metadata['k_neighbors'],
            'source': 's3' if (self.s3_client and self.is_lambda) else 'local'
        }