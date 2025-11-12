#!/usr/bin/env python3
"""
Test script for Movie Recommendation API
Demonstrates how to call the API with sample data
"""

import requests
import json

API_URL = "http://localhost:8000"


def test_health_check():
    """Test the health check endpoint"""
    print("=" * 80)
    print("Testing Health Check Endpoint")
    print("=" * 80)
    
    response = requests.get(f"{API_URL}/health")
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    print()


def test_recommendations_no_ratings():
    """Test recommendations with no user ratings (cold start)"""
    print("=" * 80)
    print("Test 1: New User with No Ratings (Cold Start)")
    print("=" * 80)
    
    payload = {
        "user_ratings": {},
        "n": 5
    }
    
    response = requests.post(f"{API_URL}/recommendations", json=payload)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"Strategy: {result['strategy']}")
        print(f"Total recommendations: {result['total']}")
        print("\nRecommendations:")
        for rec in result['recommendations']:
            print(f"  {rec['title']} (Rating: {rec['predicted_rating']:.2f})")
    else:
        print(f"Error: {response.text}")
    print()


def test_recommendations_few_ratings():
    """Test recommendations with a few ratings (hybrid approach)"""
    print("=" * 80)
    print("Test 2: User with Few Ratings (Hybrid Approach)")
    print("=" * 80)
    
    payload = {
        "user_ratings": {
            1: 5.0,    # Toy Story
            50: 4.5,   # Usual Suspects
            32: 2.0    # 12 Monkeys
        },
        "n": 5
    }
    
    print("User ratings:")
    print("  Toy Story: 5.0")
    print("  Usual Suspects: 4.5")
    print("  12 Monkeys: 2.0")
    print()
    
    response = requests.post(f"{API_URL}/recommendations", json=payload)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"Strategy: {result['strategy']}")
        print(f"Total recommendations: {result['total']}")
        print("\nRecommendations:")
        for rec in result['recommendations']:
            print(f"  {rec['title']} (Rating: {rec['predicted_rating']:.2f})")
    else:
        print(f"Error: {response.text}")
    print()


def test_recommendations_many_ratings():
    """Test recommendations with many ratings (personalized)"""
    print("=" * 80)
    print("Test 3: User with Many Ratings (Fully Personalized)")
    print("=" * 80)
    
    payload = {
        "user_ratings": {
            1: 5.0,      # Toy Story
            50: 4.5,     # Usual Suspects
            32: 2.0,     # 12 Monkeys
            110: 4.0,    # Braveheart
            260: 5.0,    # Star Wars: Episode IV
            356: 3.5,    # Forrest Gump
            296: 5.0     # Pulp Fiction
        },
        "n": 10,
        "k": 10
    }
    
    print(f"User ratings: {len(payload['user_ratings'])} movies")
    print()
    
    response = requests.post(f"{API_URL}/recommendations", json=payload)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"Strategy: {result['strategy']}")
        print(f"Total recommendations: {result['total']}")
        print("\nRecommendations:")
        for rec in result['recommendations']:
            print(f"  {rec['title']} (Rating: {rec['predicted_rating']:.2f})")
    else:
        print(f"Error: {response.text}")
    print()


def test_invalid_rating():
    """Test validation with invalid rating value"""
    print("=" * 80)
    print("Test 4: Invalid Rating Value (Should Fail)")
    print("=" * 80)
    
    payload = {
        "user_ratings": {
            1: 10.0  # Invalid: rating must be 0.5-5.0
        },
        "n": 5
    }
    
    response = requests.post(f"{API_URL}/recommendations", json=payload)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Movie Recommendation API - Test Suite")
    print("=" * 80)
    print(f"Testing API at: {API_URL}")
    print("Make sure the API is running (python run_local.py)")
    print("=" * 80)
    print()
    
    try:
        test_health_check()
        test_recommendations_no_ratings()
        test_recommendations_few_ratings()
        test_recommendations_many_ratings()
        test_invalid_rating()
        
        print("=" * 80)
        print("All tests completed!")
        print("=" * 80)
        
    except requests.exceptions.ConnectionError:
        print("\n❌ ERROR: Could not connect to API")
        print("Make sure the API is running with: python run_local.py")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
