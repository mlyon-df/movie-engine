#!/usr/bin/env python3
"""
Local development server for Movie Recommendation API
Run this script to test the API locally before deploying to Lambda
"""

import uvicorn
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

if __name__ == "__main__":
    print("=" * 80)
    print("Starting Movie Recommendation API (Local Development Server)")
    print("=" * 80)
    print("\nAPI will be available at: http://localhost:8000")
    print("Interactive docs (Swagger UI): http://localhost:8000/docs")
    print("Alternative docs (ReDoc): http://localhost:8000/redoc")
    print("\nPress CTRL+C to stop the server")
    print("=" * 80)
    print()
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload on code changes
        log_level="info"
    )
