import os, sys, math, requests
import json
from pymongo import MongoClient, UpdateOne, ASCENDING
import glob
import gzip
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# ---------------------------
# Boot / env
# ---------------------------
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

headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_KEY}"}

COLLECTION = "tmdb_series"

# ---------------------------
# Mongo helpers
# ---------------------------


# FETCH THE DATABASE COLLECTION
def fetch_db_collection():
    mongo = MongoClient(MONGO_URI)
    collection = mongo[DB_NAME][COLLECTION]
    collection.create_index([("id", ASCENDING)], unique=True)
    return collection


def fetch_db_series():
    c = fetch_db_collection()
    return c.find()


def fetch_series_from_mongodb():
    """Kept for compatibility; used only for counts in logs if needed."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION]
    return list(collection.find())


# ---------------------------
# Files: latest fN.json for series
# ---------------------------
def fetch_most_recent_file():
    if not os.path.exists("app/series_files"):
        os.makedirs("app/series_files")

    files = glob.glob(os.path.join(os.curdir, "app/series_files", "f*.json"))

    if not files:
        print("fetch_most_recent_file: No files found")
        return None

    files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))
    return files[-1]

# ---------------------------
# Files: most recent fN.json availble (locally)
# ---------------------------
def fetch_most_recent_file_name():
    if not os.path.exists("app/series_files"):
        os.makedirs("app/series_files")

    files = glob.glob(
        os.path.join(os.path.abspath(os.curdir), "app/series_files", "f*.json")
    )

    if not files:
        return None

    files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))
    return os.path.splitext(os.path.basename(files[-1]))[0]

# File's lenght
def fetch_tmdb_series_length():
    path = fetch_most_recent_file()
    if not path:
        return 0
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)

# Partitionize [parts] the number of line to process
def partitions(total: int, parts: int):
    size = math.ceil(total / parts)
    out = []
    for i in range(parts):
        start = i * size
        end = min(start + size, total)
        if start < end:
            out.append((start, end, i + 1))  # (start, end, part_index)
    return out


# ---------------------------
# TMDB API calls (TV)
# ---------------------------
def fetch_serie_details(serie_id: int):
    url = f"https://api.themoviedb.org/3/tv/{serie_id}?api_key={TMDB_API_KEY}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        # removed / not found
        return None
    else:
        print(f"Error fetching serie details for ID {serie_id}: {response.status_code}")
        return None


def fetch_all_titles(serie_id: int):
    url = f"https://api.themoviedb.org/3/tv/{serie_id}/alternative_titles?api_key={TMDB_API_KEY}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"Error fetching serie alternative titles for ID {serie_id}: {response.status_code}"
        )
        return None


# ---------------------------
# DB info helpers
# ---------------------------
def fetch_last_known_serie_id():
    c = fetch_db_collection()
    last_serie = c.find_one(sort=[("id", -1)])
    if last_serie:
        return last_serie["id"]
    else:
        return None


# ---------------------------
# Download latest TMDB export for series
# ---------------------------
def dl_recent_series_ids():
    now = datetime.now()

    file_name = now.strftime("tv_series_ids_%m_%d_%Y.json.gz")
    file_name_a = now.strftime("adult_tv_series_ids_%m_%d_%Y.json.gz")
    print(f"Trying to download the file: {file_name} and {file_name_a}")
    url = f"http://files.tmdb.org/p/exports/{file_name}"
    url_a = f"http://files.tmdb.org/p/exports/{file_name_a}"

    response = requests.get(url, stream=True)
    response_a = requests.get(url_a, stream=True)

    i = 0
    while response.status_code != 200:
        i += 1
        print(f"Failed to download series file. Status code: {response.status_code}")
        print(f"Trying to download the day -{i} file.")
        previous_date = now - timedelta(days=i)
        file_name = previous_date.strftime("tv_series_ids_%m_%d_%Y.json.gz")
        url = f"http://files.tmdb.org/p/exports/{file_name}"
        response = requests.get(url, stream=True)
        if i == 20:
            print("No file found in the last 20 days for series.")
            return

    i_a = 0
    while response_a.status_code != 200:
        i_a += 1
        print(
            f"Failed to download adult serie file. Status code: {response_a.status_code}"
        )
        print(f"Trying to download the day -{i_a} file.")
        previous_date = now - timedelta(days=i_a)
        file_name_a = previous_date.strftime("adult_tv_series_ids_%m_%d_%Y.json.gz")
        url_a = f"http://files.tmdb.org/p/exports/{file_name_a}"
        response_a = requests.get(url_a, stream=True)
        if i_a == 20:
            print("No file found in the last 20 days for series adult.")
            return

    if response.status_code == 200 and response_a.status_code == 200:
        if fetch_most_recent_file_name() is None:
            f_name = "f1.json"
        else:
            f_name = (
                "f"
                + str(
                    int(
                        re.search(
                            r"f(\d+)", fetch_most_recent_file_name()
                        ).group(1)
                    )
                    + 1
                )
                + ".json"
            )

        # Combine the responses directly into the final file
        out_path = f"{os.curdir}/app/series_files/{f_name}"
        with open(out_path, "wb") as f_out:
            f_out.write(gzip.decompress(response.content))
            f_out.write(gzip.decompress(response_a.content))
            
        # Check if there are more than 2 files in the directoryn, delete the oldest one
        files = glob.glob(os.path.join(os.curdir + "/app/series_files/", "f*.json"))
        if len(files) > 2:
            files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))
            os.remove(files[0])
        print(f"File downloaded and extracted successfully to {out_path}.")


# ---------------------------
# Images
# ---------------------------
def fetch_serie_image(serie_id, path):
    url = f"https://image.tmdb.org/t/p/w500/{path}"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        os.makedirs("app/series_img", exist_ok=True)
        image_path = os.path.join("app/series_img", f"{serie_id}.jpg")
        with open(image_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=128):
                f.write(chunk)
        return image_path
    else:
        return None


def dl_serie_images():
    series = fetch_db_series()
    c = fetch_db_collection()
    for s in series:
        if s.get("poster_path"):
            image_path = fetch_serie_image(s["id"], s["poster_path"])
            if image_path:
                c.update_one({"id": s["id"]}, {"$set": {"poster_path": image_path}})
                print(
                    f"Image downloaded for serie \"{s['original_name']}\" with id \"{s['id']}\""
                )
            else:
                print(
                    f"Error downloading image for serie \"{s['original_name']}\" with id \"{s['id']}\""
                )


# ---------------------------
# Series doc
# ---------------------------
def serie_doc(l_s, details):
    return {
        "id": l_s["id"],
        "original_name": l_s["original_name"],
        "name": details["name"]
        if details.get("name") is not None
        else l_s["original_name"],
        "created_by": details.get("created_by"),
        "poster_path": details.get("poster_path"),
        "all_titles": fetch_all_titles(l_s["id"]),
        "adult": details.get("adult"),
        "overview": details.get("overview"),
        "first_air_date": details.get("first_air_date"),
        "last_air_date": details.get("last_air_date"),
        "next_episode_to_air": details.get("next_episode_to_air"),
        "networks": details.get("networks"),
        "in_production": details.get("in_production"),
        "seasons": details.get("seasons"),
        "number_of_seasons": details.get("number_of_seasons"),
        "number_of_episodes": details.get("number_of_episodes"),
        "last_episode_to_air": details.get("last_episode_to_air"),
        "episode_run_time": details.get("episode_run_time"),
        "genres": details.get("genres"),
        "homepage": details.get("homepage"),
        "spoken_languages": details.get("spoken_languages"),
        "production_companies": details.get("production_companies"),
        "production_countries": details.get("production_countries"),
        "status": details.get("status"),
        "popularity": details.get("popularity"),
        "backdrop_path": details.get("backdrop_path"),
        "origin_country": details.get("origin_country"),
        "original_language": details.get("original_language"),
    }


# ---------------------------
# THREADED compare file vs DB with UPSERT
# ---------------------------
def sync_series_file_add_db_threaded(parts: int = 4, only_new: bool = True):
    """Threaded compare+UPSERT for series.

    only_new=True  -> skip ids <= last_id (current optimization)
    only_new=False -> upsert EVERYTHING from the file (no skip)
    """
    c = fetch_db_collection()

    # 1) Last id & download latest file
    try:
        last_id = fetch_last_known_serie_id()
        print(f"Last known serie ID in the database: {last_id}")
    except Exception as e:
        print(f"Error fetching last known serie ID: {e}")
        last_id = None

    dl_recent_series_ids()

    series_file = fetch_most_recent_file()
    if series_file is None:
        print("No serie file has been downloaded, try checking back the API caller!")
        return

    with open(series_file, "r", encoding="utf-8") as f:
        file_lines = f.readlines()

    db_len = c.count_documents({})
    file_len = len(file_lines)
    print(
        f" Number of series in the database: {db_len}"
        + f"\n Number of series in the file: {file_len}"
    )

    ranges = partitions(file_len, parts)
    print(f"[INFO] Using {len(ranges)} partitions for TMDB series file of {file_len} lines")

    def worker(start: int, end: int, part: int) -> int:
        upserted_count = 0
        for idx in range(start, end):
            line = file_lines[idx]
            try:
                l_s = json.loads(line)
            except json.JSONDecodeError:
                print(f"[Part {part}] JSON error at line {idx}")
                continue

            sid = l_s.get("id")
            if not isinstance(sid, int):
                continue

            # 🔥 optional skip of old ids
            if only_new and last_id is not None and sid <= last_id:
                continue

            details = fetch_serie_details(sid)
            if details and isinstance(details.get("id"), int):
                doc = serie_doc(l_s, details)
                c.update_one({"id": sid}, {"$set": doc}, upsert=True)
                print(
                    f"[Part {part}] Serie UPSERTED \"{l_s.get('original_name', sid)}\" with id \"{sid}\""
                )
                upserted_count += 1

            elif not details:
                db_existing_serie = c.find_one({"id": sid})
                if db_existing_serie:
                    c.delete_one({"id": sid})
                    print(
                        f"[Part {part}] Serie DELETED \"{l_s.get('original_name', sid)}\" with id \"{sid}\""
                    )

        return upserted_count

    total_upserted = 0
    with ThreadPoolExecutor(max_workers=parts) as pool:
        futures = {
            pool.submit(worker, s, e, p): (s, e, p) for (s, e, p) in ranges
        }
        for fut in as_completed(futures):
            s, e, p = futures[fut]
            upserted = fut.result()
            total_upserted += upserted
            print(f"[DONE] Part {p} ({s}-{e}) upserted {upserted} series.")

    print(f"[DONE] Total series upserted this run: {total_upserted}")
    
    if __name__ == "__main__":
        # You may adjust parts and only_new as needed
        sync_series_file_add_db_threaded(parts=10, only_new=True)
