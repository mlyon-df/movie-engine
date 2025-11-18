# Movie Recommender Frontend

A mobile-first React application for getting personalized movie recommendations.

## Features

- 🎬 Personalized movie recommendations using Item-Based Collaborative Filtering
- 📱 Mobile-first, responsive design optimized for portrait mode
- 💾 Local storage to persist user ratings
- 🎨 Clean, modern UI with movie posters from TMDB
- ⚡ Fast and smooth user experience
- 🔄 Real-time recommendation updates based on ratings

## Rating System

- **Red Frown Face (😞)**: 1 star - Dislike
- **Yellow Neutral Face (😐)**: 3 stars - Neutral
- **Green Smile (😊)**: 5 stars - Like

## Getting Started

### Prerequisites

- Node.js 16+ installed
- Movie Engine API running (default: http://localhost:8000)

### Installation

1. Install dependencies:
```bash
npm install
```

2. (Optional) Configure API URL:
Create a `.env` file in the root directory:
```
VITE_API_URL=http://localhost:8000
```

If not specified, it defaults to `http://localhost:8000`.

### Running the App

Development mode with hot reload:
```bash
npm run dev
```

The app will open at http://localhost:3000

Build for production:
```bash
npm run build
```

Preview production build:
```bash
npm run preview
```

## How It Works

1. **Initial Load**: The app fetches 10 movie recommendations from the API
2. **Rating**: User rates a movie by clicking one of the three face buttons
3. **Storage**: Rating is saved to browser's local storage
4. **Refresh**: New recommendations are fetched based on updated ratings
5. **Skip**: User can skip to the next movie in the current recommendation list
6. **Continuous Flow**: When all movies are rated or skipped, new recommendations are loaded

## Project Structure

```
movie-engine-fe/
├── src/
│   ├── components/
│   │   ├── MovieCard.jsx       # Main movie display component
│   │   └── MovieCard.css       # Movie card styles
│   ├── services/
│   │   ├── api.js              # API communication
│   │   └── storage.js          # Local storage management
│   ├── App.jsx                 # Main app component
│   ├── App.css                 # App-level styles
│   ├── main.jsx                # React entry point
│   └── index.css               # Global styles
├── index.html                  # HTML template
├── vite.config.js              # Vite configuration
└── package.json                # Dependencies and scripts
```

## API Integration

The frontend expects the following API endpoint:

### POST `/recommendations`

Request body:
```json
{
  "user_ratings": {
    "1": 5.0,
    "50": 3.0,
    "100": 1.0
  },
  "n": 10,
  "k": 10
}
```

Response:
```json
{
  "recommendations": [
    {
      "movieId": 1,
      "title": "Movie Title (2001)",
      "predicted_rating": 4.5,
      "poster_url": "https://...",
      "overview": "Movie description..."
    }
  ],
  "total": 10,
  "strategy": "personalized"
}
```

## Local Storage

User ratings are stored in browser's local storage under the key `movie-engine-ratings`:

```json
{
  "1": 5.0,
  "50": 3.0,
  "100": 1.0
}
```

## Technologies Used

- **React 18** - UI framework
- **Vite** - Build tool and dev server
- **CSS3** - Styling with modern features
- **Local Storage API** - Client-side data persistence
- **Fetch API** - HTTP requests

## Browser Support

- Chrome/Edge 90+
- Firefox 88+
- Safari 14+
- Mobile browsers (iOS Safari, Chrome Mobile)

## License

Part of the Movie Engine project.
