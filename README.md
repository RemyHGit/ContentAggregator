# ContentAggregator
ContentAggregator is a project which fetch all the movies and series from the famous website **"TMDB"**, all the games from **IGDB** and all the books from the **Google Books** platform, store it to a docker's MongoDB database for further uses.

## How it works 
### Movies / Series
That project works as follow : getting public movies id ; which anyone can download, clean them, ask the TMDB, IGDB and Google Books API for getting more details (useful ones), download every poster of movies / series / games if that's what you want and put all of theses data in a MongoDB database.

All that thing is working with threads, so fetching movies, series, games and books is simultaneous.

### Games
For games, we're using the IGDB API which requires Twitch credentials. The script fetches games in batches and syncs them to MongoDB with threading support.

### Books
For books, we're using the Google Books API (free, no API key needed). The script searches books by various categories and keywords, then fetches detailed information for each book and stores it in MongoDB with threading support.

### Automation with Airflow
The project is now automated using **Apache Airflow**. Each content type (movies, series, games, books) has its own DAG that runs daily to sync data from the APIs to MongoDB. There's also a `sync_all_sources` DAG that runs all syncs in parallel.

## How to run it

### Manual execution
Put that line in the terminal at the root of the project : 
```bash
pip install -r requirements.txt
```

Then, run `plugins/main.py` and it should works. This will sync all sources (movies, series, games, books) using threads.

### With Airflow (Docker)
The project includes a `docker-compose.yml` file to run Airflow. Just run:
```bash
docker-compose up
```

Then access the Airflow UI at `http://localhost:8080` (default credentials: airflow/airflow).

The DAGs available are:
- `sync_tmdb_movies` - Sync movies from TMDB
- `sync_tmdb_series` - Sync series from TMDB  
- `sync_igdb_games` - Sync games from IGDB
- `sync_google_books` - Sync books from Google Books
- `sync_all_sources` - Sync all sources in parallel

## Environment variables
You need to create a `.env` file with these variables:
```python
TMDB_API_KEY = "[your TMDB api key]"
MONGO_URI = "mongodb://mongo:27017/"
DB_NAME = "msg_db"
IMAGES_DIR = "images"
TWITCH_ID = "[your TWITCH_ID]"
TWITCH_SECRET = "[your TWITCH_SECRET]"
```

Note: Google Books API doesn't require an API key, so you don't need to set anything for books.

## Future for that project
Adjust the code so that you get what you want, like "i only want movies" ;  "okay, here we go:".

My goal for that project is to achieve a way on getting some data on anything from website legally and for everyone, hope it's going anywhere with that idea.