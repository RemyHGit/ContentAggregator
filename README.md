# ContentAggregator
ContentAggregator is a project which fetch all the movies and series from the famous website **"TMDB"**, all the games from **IGDB**, and all the music artists from **MusicBrainz**, store it to a docker's MongoDB database for further uses.

## How it works 
### Movies / Series
That project works as follow : getting public movies id ; which anyone can download, clean them, ask the TMDB, IGDB and MusicBrainz API for getting more details (useful ones), download every poster of movies / series / games if that's what you want and put all of theses data in a MongoDB database.

All that thing is working with threads, so fetching movies, series, games and music is simultaneous.

### Games
For games, we're using the IGDB API which requires Twitch credentials. The script fetches games in batches and syncs them to MongoDB with threading support.

### Music
For music, we're using the MusicBrainz API (free, no API key needed, but requires User-Agent header). The script searches artists by various queries, fetches their releases (albums), and stores all information in MongoDB. The API has a rate limit of 1 request per second which is automatically handled.

### Openlibrary
For books, we're using the **Open Library** data dumps. The script downloads the editions dump, processes book records, and stores them in MongoDB. By default it only fetches new records that have not already been synced, and downloading/parsing is automated. Book syncing is threaded for speed, and book covers can also be fetched if configured.


### Automation with Airflow
The project is now automated using **Apache Airflow**. Each content type (movies, series, games, music) has its own DAG that runs daily to sync data from the APIs to MongoDB. There's also a `sync_all_sources` DAG that runs all syncs in parallel.

## How to run it

### Manual execution
Put that line in the terminal at the root of the project : 
```bash
pip install -r requirements.txt
```

Then, run `plugins/main.py` and it should works. This will sync all sources (movies, series, games, music) using threads.

### With Airflow (Docker)
The project includes a `docker-compose.yml` file to run Airflow. Just run:
```bash
docker-compose up --build -d
```

It will build and create a docker container, containing what's needed for running the file !


Then access the Airflow UI at `http://localhost:8080` (default credentials: admin/admin).

The DAGs available are:
- `sync_tmdb_movies` - Sync movies from TMDB
- `sync_tmdb_series` - Sync series from TMDB  
- `sync_igdb_games` - Sync games from IGDB
- `sync_brainz_music` - Sync music artists from MusicBrainz
- `sync_openlibrary_books` - Sync books from Openlibrary
- `sync_all_sources` - Sync all sources in parallel

## Environment variables
You need to create a `.env` file, a sample is already named as `.env.exemple`.

## Future for that project
Adding more and more data !

My goal for that project is to achieve a way on getting some data on anything from website legally and for everyone, hope it's going anywhere with that idea.