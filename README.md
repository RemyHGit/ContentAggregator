# ContentAggregator

ContentAggregator is a project which fetch all the movies and series from the famous website **"TMDB"**, all the games from **"IGDB"**, and all the music artists from **"MusicBrainz"** and all the books  from a dump extract of **"Openlibrary"** store it to a docker's MongoDB database for further uses.

## Purpose of that project

That project is meant to be part of my Master's final exam project, it will get various data from many sources, treat them, and be used on a personal website ;
The website will then catch up on these data, treat them in return and categorize them using a powerful AI gender recommendation  

## How it works

That project works as follow : getting public movies id ; which anyone can download, clean them, ask the TMDB, IGDB, MusicBrainz and Openlibrary API for getting more details (useful ones), download every poster of movies / series / games if that's what you want and put all of theses data in a MongoDB database.

All that thing is working with threads, so fetching movies, series, games and music is simultaneous.

### Movies / Series

For movies and series, we're using the **TMDB (The Movie Database)** API, which provides rich metadata for both films and television shows. To access the TMDB API, you'll need an API key, which you can obtain for free by creating an account at [TMDB](https://www.themoviedb.org/).

The script will:

- Fetch lists of popular and new movies and series, or all items matching public TMDB IDs.
- Use threading to parallelize API calls for faster data retrieval.
- Collect detailed metadata for each movie or series, including titles, genres, ratings, release dates, cast, crew, overviews, and poster images.
- Download and store poster/cover images (if configured) in your local app data directory.
- Store all data in MongoDB, ensuring that only new or updated records are added to avoid duplicates.

Both movies and series sync jobs are available as separate Airflow DAGs (`sync_tmdb_movies` and `sync_tmdb_series`), so you can schedule them independently as needed.
  
TMDB's API has generous rate limits, but if you sync a large number of movies or series, the script automatically adheres to TMDB's guidelines about requests per second.

All TMDB-related configuration (such as API key and language) should be set in the `.env` file.

### Games

For games, we're using the **IGDB** (Internet Game Database) API, which provides detailed metadata for video games. To use the IGDB API, you'll need to obtain a `client_id` and `client_secret` by creating an application on the [Twitch Developer Portal](https://dev.twitch.tv/console/apps). The script uses these credentials to get an access token required for all IGDB API calls.

The script will:

- Fetch games in configurable batches, ensuring broad coverage of game titles.
- Use threading to parallelize API requests for rapid data retrieval and syncing.
- Collect detailed metadata for each game, including titles, genres, platforms, release dates, developers, and cover/box art images.
- Download and store cover images (if configured) in your local app data directory.
- Store all collected game data in MongoDB, ensuring only new or updated games are inserted to avoid duplicates.

The IGDB sync is managed via an Airflow DAG (`sync_igdb_games`), so you can schedule regular updates. The script automatically manages rate limits and retries to comply with API requirements, ensuring efficient and robust synchronization.

All IGDB-related configuration (such as client ID/secret) should be set in the `.env` file.

### Music

For music, we're using the **MusicBrainz** API. No API key is required, but the API needs a User-Agent header. The script:

- Searches for music artists using configurable queries.
- Uses threading to parallelize requests and speed up data retrieval.
- Fetches detailed artist data along with their releases (albums) using the MusicBrainz API, while ensuring the required rate limit of 1 request per second is respected automatically.
- Stores all artist and album data in MongoDB, updating only new or modified artists to avoid duplicates.

Music sync is managed by an Airflow DAG (`sync_brainz_music`) for scheduled runs. Configuration (such as queries and language) can be set in the `.env` file.

### Books

For books, we're using the **Open Library** data dumps. The script:

- Downloads the latest Open Library editions dump from the public source.
- Processes and parses the dump, extracting detailed book metadata like titles, authors, editions, publication dates, and identifiers.
- Uses threading to speed up processing and sync only new or updated book records to MongoDB, avoiding duplicates.
- Optionally, book cover images can be downloaded and stored if configured.
- Automates download, parsing, and syncing, so you don't need to manually manage the dump file.

Book synchronization is managed via its own Airflow DAG (`sync_openlibrary_books`), which you can schedule as needed.

### Automation with Airflow

The project is automated using **Apache Airflow**. Each content type (movies, series, games, music, books) has its own DAG that runs daily, independently syncing data from each external API or source to MongoDB. There is also a `sync_all_sources` DAG that runs all sync tasks in parallel for full data updates.

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

- `sync_tmdb_movies` - Sync movies from **TMDB**
- `sync_tmdb_series` - Sync series from **TMDB**
- `sync_igdb_games` - Sync games from **IGDB**
- `sync_brainz_music` - Sync music artists from **MusicBrainz**
- `sync_openlibrary_books` - Sync books from **Openlibrary**
- `sync_all_sources` - Sync all sources in parallel

## Environment variables

You need to create a `.env` file, a sample is already named as `.env.exemple`.

## Future for that project

Adding more and more data !

My goal for that project is to achieve a way on getting some data on anything from website legally and for everyone, hope it's going anywhere with that idea.
