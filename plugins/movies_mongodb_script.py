import os, sys, math, requests
import json
from pymongo import MongoClient, UpdateOne, ASCENDING
import glob
import gzip
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import threading

# boot
sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise RuntimeError("TMDB_API_KEY is not set in your .env")

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in your .env")

DB_NAME = os.getenv("DB_NAME")
if not DB_NAME:
    raise RuntimeError("DB_NAME is not set in your .env")

COLLECTION = "tmdb_movies"

headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_KEY}"}

# mongo
def fetch_db_collection():
    """Fetches and returns the MongoDB collection for movies, creating index if needed"""
    mongo = MongoClient(MONGO_URI)
    collection = mongo[DB_NAME][COLLECTION]
    collection.create_index([("id", ASCENDING)], unique=True)
    return collection

def fetch_db_movies():
    """Fetches all movies from MongoDB collection"""
    c = fetch_db_collection()
    return c.find()

# files
def fetch_most_recent_file():
    """Returns the path to the most recent movie IDs file (fN.json)"""
    if not os.path.exists("app/movies_files"):
        os.makedirs("app/movies_files")
    
    files = glob.glob(os.path.join(os.curdir, "app/movies_files", "f*.json"))

    if not files:
        print("fetch_most_recent_file: No files found")
        return None

    files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))
    return files[-1]

def fetch_most_recent_file_name():
    """Returns the name (without extension) of the most recent movie IDs file"""
    if not os.path.exists("app/movies_files"):
        os.makedirs("app/movies_files")
    
    files = glob.glob(os.path.join(os.path.abspath(os.curdir), "app/movies_files", "f*.json"))

    if not files:
        return None

    files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))
    return os.path.splitext(os.path.basename(files[-1]))[0]

def fetch_tmdb_movies_length():
    """Returns the number of lines in the most recent movie IDs file"""
    path = fetch_most_recent_file()
    if not path:
        return 0
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)

# partitions
def partitions(total: int, parts: int):
    """Divides a total into multiple partitions for parallel processing"""
    size = math.ceil(total / parts)
    out = []
    for i in range(parts):
        start = i * size
        end = min(start + size, total)
        if start < end:
            out.append((start, end, i + 1))
    return out

# api
def fetch_movie_details(movie_id: int):
    """Fetches complete movie details from TMDB API by movie ID"""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}"
    response = requests.get(url, headers=headers)
    data = response.json()

    if response.status_code == 200:
        return data
    else:
        print(f"Error fetching movie details for ID {movie_id}: {response.status_code}")
        return None

def fetch_all_titles(movie_id: int):
    """Fetches all alternative titles for a movie from TMDB API"""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/alternative_titles?api_key={TMDB_API_KEY}"
    response = requests.get(url, headers=headers)

    # Check for response status code
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error fetching movie details for ID {movie_id}: {response.status_code}")
        return None

def fetch_movies_from_mongodb():
    """Fetches all movies from MongoDB and returns them as a list"""
    client = MongoClient(MONGO_URI)    
    db = client[DB_NAME]
    collection = db["movies"]

    movies_data = []
    for document in collection.find():
        movies_data.append(document)
    return movies_data

# helpers
def is_anime(details):
    """Detects if a series/movie is an anime based on genre and origin country"""
    genres = details.get("genres", [])
    origin_country = details.get("origin_country", [])
    production_countries = details.get("production_countries", [])
    
    # Check for Animation genre (ID 16 in TMDB)
    has_animation = any(genre.get("id") == 16 for genre in genres if isinstance(genre, dict))
    
    # Check for Japan as origin/production country
    is_japanese = False
    if isinstance(origin_country, list):
        is_japanese = "JP" in origin_country
    if not is_japanese and isinstance(production_countries, list):
        is_japanese = any(
            country.get("iso_3166_1") == "JP" 
            for country in production_countries 
            if isinstance(country, dict)
        )
    
    # Consider it anime if it has Animation genre AND is from Japan
    return has_animation and is_japanese

def fetch_last_known_movie_id():
    """Returns the highest movie ID currently in the database"""
    c = fetch_db_collection()
    last_movie = c.find_one(sort=[("id", -1)])
    if last_movie:
        return last_movie["id"]
    else:
        return None

# download
def dl_recent_movie_ids():
    """Downloads the most recent TMDB movie IDs export files (regular and adult)"""
    now = datetime.now()

    file_name = now.strftime("movie_ids_%m_%d_%Y.json.gz")
    file_name_a = now.strftime("adult_movie_ids_%m_%d_%Y.json.gz")
    print(f"Trying to download the file: {file_name} and {file_name_a}")
    url = f"http://files.tmdb.org/p/exports/{file_name}"
    url_a = f"http://files.tmdb.org/p/exports/{file_name_a}"

    response = requests.get(url, stream=True)
    response_a = requests.get(url_a, stream=True)

    i = 0
    while response.status_code != 200:
        i += 1
        print(f"Failed to download movies file. Status code: {response.status_code}")
        print(f"Trying to download the day -{i} file.")
        previous_date = now - timedelta(days=i)
        file_name = f"movie_ids_{previous_date.month}_{previous_date.day}_{previous_date.year}.json.gz"
        url = f"http://files.tmdb.org/p/exports/{file_name}"
        response = requests.get(url, stream=True)
        if i == 20:
            print("No file found in the last 20 days for movies.")
            return

    i_a = 0
    while response_a.status_code != 200:
        i_a += 1
        print(f"Failed to download adult file. Status code: {response_a.status_code}")
        print(f"Trying to download the day -{i_a} file.")
        previous_date = now - timedelta(days=i_a)
        file_name_a = f"adult_movie_ids_{previous_date.month}_{previous_date.day}_{previous_date.year}.json.gz"
        url_a = f"http://files.tmdb.org/p/exports/{file_name_a}"
        response_a = requests.get(url_a, stream=True)
        if i_a == 20:
            print("No file found in the last 20 days for movies adult.")
            return

    if response.status_code == 200 and response_a.status_code == 200:
        # Create the file name of the file that will contain all the movies
        if fetch_most_recent_file_name() is None:
            f = "f1.json"
        else:
            f = (
                "f"
                +str(
                    int(re.search(r"f(\d+)", fetch_most_recent_file_name()).group(1))
                    +1
                )
                +".json"
            )

        # Combine the responses directly into the final file
        with open(f"{os.curdir}/app/movies_files/{f}", "wb") as f_out:
            f_out.write(gzip.decompress(response.content))
            f_out.write(gzip.decompress(response_a.content))

        # Check if there are more than 2 files in the directoryn, delete the oldest one
        files = glob.glob(os.path.join(os.curdir + "/app/movies_files/", "f*.json"))
        if len(files) > 2:
            files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))
            os.remove(files[0])
        print("File downloaded and extracted successfully.")

def check_movie_attributes(movie_details, db_movie_details):
    """Checks if movie attributes have changed between API data and database data"""
    attributes_to_check = [
        "id",
        "original_title",
        "poster_path",
        "all_titles",
        "adult",
        "overview",
        "release_date",
        "runtime",
        "genres",
        "spoken_languages",
        "production_companies",
        "production_countries",
        "status",
        "popularity",
        "video",
        "backdrop_path",
        "budget",
        "origin_country",
        "original_language"
    ]

    for attr in attributes_to_check:
        if attr == "all_titles":
            continue
        if db_movie_details.get(attr) != movie_details.get(attr):
            return False
    return True

# images
def fetch_movie_image(movie_id, path):
    """Downloads a movie poster image from TMDB and saves it locally"""
    url = f"https://image.tmdb.org/t/p/w500/{path}"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        os.makedirs("images/movies", exist_ok=True)
        image_path = os.path.join("images/movies", f"{movie_id}.jpg")
        with open(image_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=128):
                f.write(chunk)
        return image_path
    else:
        return None

def dl_movie_images():
    """Downloads poster images for all movies in the database"""
    movies = fetch_db_movies()
    for m in movies:
        if m["poster_path"]:
            image_path = fetch_movie_image(m["id"], m["poster_path"])
            if image_path:
                fetch_db_collection().update_one(
                    {"id": m["id"]}, {"$set": {"poster_path": image_path}}
                )
                print(f"Image downloaded for movie \"{m['original_title']}\" with id \"{m['id']}\"")
            else:
                print(f"Error downloading image for movie \"{m['original_title']}\" with id \"{m['id']}\"")

# doc
def movie_doc(l_m, details):
    """Creates a structured MongoDB document from TMDB movie data"""
    return {
        "id": l_m["id"],
        "original_title": l_m["original_title"],
        "poster_path": details["poster_path"] if details.get("poster_path") is not None else None,
        "all_titles": fetch_all_titles(l_m["id"]),
        "adult": l_m["adult"],
        "overview": details["overview"] if details.get("overview") is not None else None,
        "release_date": details["release_date"] if details.get("release_date") is not None else None,
        "runtime": details["runtime"] if details.get("runtime") is not None else None,
        "genres": details["genres"] if details.get("genres") is not None else None,
        "spoken_languages": details["spoken_languages"] if details.get("spoken_languages") is not None else None,
        "production_companies": details["production_companies"] if details.get("production_companies") is not None else None,
        "production_countries": details["production_countries"] if details.get("production_countries") is not None else None,
        "status": details["status"] if details.get("status") is not None else None,
        "popularity": details["popularity"] if details.get("popularity") is not None else None,
        "video": details["video"] if details.get("video") is not None else None,
        "backdrop_path": details["backdrop_path"] if details.get("backdrop_path") is not None else None,
        "budget": details["budget"] if details.get("budget") is not None else None,
        "origin_country": details["origin_country"] if details.get("origin_country") is not None else None,
        "original_language": details["original_language"] if details.get("original_language") is not None else None,
        "is_anime": is_anime(details),
    }

# sync
def sync_movies_file_add_db_threaded(parts: int = 4, only_new: bool = True):
    """Threaded compare+UPSERT for movies."""

    c = fetch_db_collection()

    try:
        last_id = fetch_last_known_movie_id()
        print(f"Last known movie ID in the database: {last_id}")
    except Exception as e:
        print(f"Error fetching last known movie ID: {e}")
        last_id = None

    dl_recent_movie_ids()

    movie_file = fetch_most_recent_file()
    if movie_file is None:
        print("No movie file has been downloaded, try checking back the API caller!")
        return

    with open(movie_file, "r", encoding="utf-8") as f:
        file_lines = f.readlines()

    db_len = c.count_documents({})
    file_len = len(file_lines)
    print(
        f" Number of movies in the database: {db_len}"
        + f"\n Number of movies in the file: {file_len}"
    )

    ranges = partitions(file_len, parts)
    print(f"[INFO] Using {len(ranges)} partitions for TMDB movies file of {file_len} lines")

    # Event to signal threads to stop
    stop_event = threading.Event()

    def worker(start: int, end: int, part: int) -> int:
        upserted = 0
        for idx in range(start, end):
            # Check if we should stop
            if stop_event.is_set():
                print(f"[Part {part}] Stopping worker due to cancellation signal")
                break
            
            line = file_lines[idx]
            try:
                l_m = json.loads(line)
            except json.JSONDecodeError:
                print(f"[Part {part}] JSON error at line {idx}")
                continue

            mid = l_m.get("id")
            if not isinstance(mid, int):
                continue

            # Skip only if document already exists in DB (when only_new=True)
            if only_new:
                existing = c.find_one({"id": mid})
                if existing:
                    continue

            details = fetch_movie_details(mid)
            if details and isinstance(details.get("id"), int):
                doc = movie_doc(l_m, details)
                c.update_one({"id": mid}, {"$set": doc}, upsert=True)
                print(
                    f"[Part {part}] Movie UPSERTED \"{l_m.get('original_title', mid)}\" with id \"{mid}\""
                )
                upserted += 1
            elif not details:
                db_existing_movie = c.find_one({"id": mid})
                if db_existing_movie:
                    c.delete_one({"id": mid})
                    print(
                        f"[Part {part}] Movie DELETED \"{l_m.get('original_title', mid)}\" with id \"{mid}\""
                    )
        return upserted

    try:
        total_upserted = 0
        futures = {}
        with ThreadPoolExecutor(max_workers=parts) as pool:
            futures = {
                pool.submit(worker, s, e, p): (s, e, p)
                for (s, e, p) in ranges
            }
            for fut in as_completed(futures):
                s, e, p = futures[fut]
                try:
                    upserted = fut.result()
                    total_upserted += upserted
                    print(f"[DONE] Part {p} ({s}-{e}) upserted {upserted} movies.")
                except Exception as e:
                    print(f"[ERROR] Part {p} ({s}-{e}) failed: {e}")
                    stop_event.set()  # Signal other threads to stop
                    raise

        print(f"[DONE] Total movies upserted this run: {total_upserted}")
    except (KeyboardInterrupt, SystemExit):
        print("[INFO] Task cancelled, stopping all workers...")
        stop_event.set()
        # Cancel remaining futures
        if 'futures' in locals():
            for fut in futures:
                fut.cancel()
        raise

# main
if __name__ == "__main__":
    sync_movies_file_add_db_threaded(parts=10, only_new=True)
