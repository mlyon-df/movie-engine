# Movie Engine Frontend

A mobile-first React web application for discovering and rating movies through an intuitive swipe-style interface. The app provides personalized movie recommendations powered by the Movie Engine API.

## Features

- 🎬 **Personalized Recommendations**: Get movie suggestions based on your rating history using Item-Based Collaborative Filtering
- 📱 **Mobile-First Design**: Optimized for portrait mode on phones with responsive scaling
- 🎯 **Swipe-Style Interface**: Rate movies with simple tap interactions (dislike, neutral, like)
- 🎭 **Movie Details Modal**: Tap posters to view genres and overview information
- 📊 **Rating History Drawer**: View and manage all your rated movies
- 💾 **Offline Storage**: All ratings and movie data persisted in browser localStorage
- 🎨 **TMDB Integration**: Movie posters and descriptions from The Movie Database
- ⚡ **Fast & Smooth**: Clean, modern UI with smooth animations

## Rating System

- **Red Frown Face (😞)**: 1 star - Dislike
- **Yellow Neutral Face (😐)**: 3 stars - Neutral
- **Green Smile (😊)**: 5 stars - Like

## Getting Started

### Prerequisites

- Node.js 16+ and npm
- Movie Engine API running (backend)

### Installation

1. Install dependencies:
```bash
npm install
```

2. Configure the API URL:

Create a `.env` file in the project root:
```bash
VITE_API_URL=http://localhost:8000
```

For production deployment, the API URL is automatically injected by the deployment script.

### Running the App

Development mode with hot reload:
```bash
npm run dev
```

The app will be available at `http://localhost:5173`

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
2. **View Details**: Tap any movie poster to see title, year, genres, and plot overview
3. **Rating**: Rate a movie by tapping one of the three face buttons
4. **Storage**: Rating and movie metadata are saved to browser's localStorage
5. **Refresh**: New recommendations are fetched based on updated ratings
6. **Skip**: Skip to the next movie in the current recommendation list without rating
7. **Manage Ratings**: Open the drawer (menu button) to view/remove past ratings
8. **Continuous Flow**: When all movies are rated or skipped, new recommendations are loaded

## Project Structure

```
movie-engine-fe/
├── src/
│   ├── components/
│   │   ├── Drawer.jsx          # Rating history sidebar
│   │   ├── Drawer.css
│   │   ├── MovieCard.jsx       # Main movie display card
│   │   ├── MovieCard.css
│   │   ├── MovieModal.jsx      # Movie details popup
│   │   └── MovieModal.css
│   ├── services/
│   │   ├── api.js              # API client for recommendations
│   │   └── storage.js          # localStorage wrapper
│   ├── App.jsx                 # Main application component
│   ├── App.css                 # App-level styles
│   ├── main.jsx                # React entry point
│   └── index.css               # Global styles
├── public/                     # Static assets
├── index.html                  # HTML template
├── vite.config.js             # Vite configuration
└── package.json                # Dependencies and scripts
```

## API Integration

The frontend expects the following API endpoint:

### POST `/recommend`

**Request**:
```json
{
  "user_ratings": {
    "1": 5.0,
    "50": 3.0,
    "100": 1.0
  },
  "num_recommendations": 10
}
```

**Response**:
```json
{
  "recommendations": [
    {
      "movieId": 123,
      "title": "Movie Title (2024)",
      "score": 4.5,
      "poster_url": "https://image.tmdb.org/...",
      "overview": "Plot description from TMDB...",
      "genres": ["Action", "Adventure", "Sci-Fi"]
    }
  ]
}
```

## Local Storage

The app stores data in browser localStorage:

### Ratings
Key: `movie-engine-ratings`
```json
{
  "1": 5.0,
  "50": 3.0,
  "100": 1.0
}
```

### Movie Metadata
Key: `movie-engine-movies`
```json
{
  "1": {
    "title": "The Matrix",
    "year": "1999",
    "genres": ["Action", "Sci-Fi"],
    "overview": "A computer hacker learns..."
  }
}
```

## Deployment

The frontend can be deployed to AWS S3 as a static website:

```bash
# From the infra/ directory
./deploy_frontend.sh
```

This script:
1. Fetches the API Gateway URL from CloudFormation exports
2. Injects it into the build via `VITE_API_URL` environment variable
3. Builds the production bundle with `npm run build`
4. Syncs the `dist/` folder to S3 bucket `movie-engine-frontend`
5. Configures static website hosting and public read permissions

## Technologies Used

- **React 18.3.1** - UI framework
- **Vite 7.2.2** - Build tool and dev server
- **CSS3** - Styling with modern features and animations
- **localStorage API** - Client-side data persistence
- **Fetch API** - HTTP requests to backend

## Browser Support

- Chrome/Edge 90+
- Firefox 88+
- Safari 14+
- Mobile browsers (iOS Safari, Chrome Mobile)
- Requires JavaScript and localStorage enabled

## License

See the LICENSE file in the root of the repository.
