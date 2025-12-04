import threading

from movies_mongodb_script import sync_movies_file_add_db_threaded as tmdb_upsert_movies
from series_mongodb_script import sync_series_file_add_db_threaded as tmdb_upsert_series
from games_mongodb_script  import sync_all_games_threaded          as igdb_upsert_games


if __name__ == "__main__":
    try:

        # Create threads for each function
        thread_movies = threading.Thread(target=tmdb_upsert_movies, kwargs={"parts": 10, "only_new": False})
        thread_series = threading.Thread(target=tmdb_upsert_series, kwargs={"parts": 10, "only_new": False})
        thread_games =  threading.Thread(target=igdb_upsert_games,  kwargs={"parts": 4})


        # Start the threads
        thread_movies.start()
        thread_series.start()
        thread_games.start()

        # Wait for the update thread to finish
        thread_movies.join()
        thread_series.join()
        thread_games.join()
                

    except Exception as e:
        print(f"Error processing conjecture: {e}")