import requests
import json
from pymongo import MongoClient
import os
import sys
import glob
import gzip
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Set the default encoding to UTF-8
sys.stdout.reconfigure(encoding="utf-8")

# Load environment variables from .env file
load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
IMAGES_DIR = os.getenv("IMAGES_DIR")

headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_KEY}"}


# FETCH THE DATABASE COLLECTION
def fetch_db_collection():
    conn = os.getenv("MONGODB_URI", LOCAL_MONGO_URI)
    client = MongoClient(conn)
    db = client[DB_NAME]
    return db["tmdb_series"]


# FETCH ALL SERIES FROM THE DATABASE
def fetch_db_series():
    c = fetch_db_collection()
    return c.find()


# FETCH THE MOST RECENT FILE
def fetch_most_recent_file():

    # Create the directory if it doesn't exist
    if not os.path.exists("app/series_files"):
        os.makedirs("app/series_files")
    
    # Get a list of all files in the directory with the specified pattern
    files = glob.glob(os.path.join(os.curdir, "app/series_files", "f*.json"))

    if not files:
        print("fetch_most_recent_file: No files found")
        return None

    # Extract the number after the letter "f" from the file name and sort the files based on that number
    files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))

    # Return the file with the highest number after the letter "f"
    return files[-1]


# FETCH THE MOST RECENT FILE NAME
def fetch_most_recent_file_name():
    
    # Create the directory if it doesn't exist
    if not os.path.exists("app/series_files"):
        os.makedirs("app/series_files")
    # Get a list of all files in the directory with the specified pattern
    files = glob.glob(os.path.join(os.path.abspath(os.curdir), "app/series_files", "f*.json"))

    # If no files match the pattern, return None
    if not files:
        return None

    # Extract the number after the letter "f" from the file name and sort the files based on that number
    files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))

    # Return the file name with the highest number after the letter "f" without the extension
    return os.path.splitext(os.path.basename(files[-1]))[0]


# FETCH SERIES FROM MONGODB
def fetch_series_from_mongodb():
    mongodb_uri = os.getenv("MONGODB_URI", LOCAL_MONGO_URI)
    client = MongoClient(mongodb_uri)    
    db = client[DB_NAME]
    collection = db["series"]

    series_data = []
    for document in collection.find():
        series_data.append(document)
    return series_data


# FETCH SERIES DETAILS
def fetch_serie_details(serie_id):
    url = f"https://api.themoviedb.org/3/tv/{serie_id}?api_key={TMDB_API_KEY}"
    response = requests.get(url, headers=headers)
    data = response.json()

    if response.status_code == 200:
        return data
    else:
        print(f"Error fetching serie details for ID {serie_id}: {response.status_code}")
        return None  # Handle error or return empty data


# FETCH ALL TITLES OF A SERIE
def fetch_all_titles(serie_id):
    url = f"https://api.themoviedb.org/3/tv/{serie_id}/alternative_titles?api_key={TMDB_API_KEY}"
    response = requests.get(url, headers=headers)

    # Check for response status code
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error fetching serie details for ID {serie_id}: {response.status_code}")
        return None  # Handle error or return empty data


# FETCH THE LAST KNOWN SERIE ID IN THE DATABASE
def fetch_last_known_serie_id():
    c = fetch_db_collection()
    last_serie = c.find_one(sort=[("id", -1)])
    if last_serie:
        return last_serie["id"]
    else:
        return None


# DOWNLOAD THE MOST RECENT SERIE AND ADULT SERIE IDS FILE
def dl_recent_series_ids():
    now = datetime.now()

    # Try to download the file for the current date
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
        file_name = f"tv_series_ids_{previous_date.month}_{previous_date.day}_{previous_date.year}.json.gz"
        url = f"http://files.tmdb.org/p/exports/{file_name}"
        response = requests.get(url, stream=True)
        if i == 20:
            print("No file found in the last 20 days for series.")
            return

    i_a = 0
    while response_a.status_code != 200:
        i_a += 1
        print(f"Failed to download adult serie file. Status code: {response_a.status_code}")
        print(f"Trying to download the day -{i_a} file.")
        previous_date = now - timedelta(days=i_a)
        file_name_a = f"adult_tv_series_ids_{previous_date.month}_{previous_date.day}_{previous_date.year}.json.gz"
        url_a = f"http://files.tmdb.org/p/exports/{file_name_a}"
        response_a = requests.get(url_a, stream=True)
        if i_a == 20:
            print("No file found in the last 20 days for series adult.")
            return

    if response.status_code == 200 and response_a.status_code == 200:
        # Create the file name of the file that will contain all the movies
        if fetch_most_recent_file_name() is None:
            f = "f1.json"
        else:
            f = (
                "f"
                + str(
                    int(re.search(r"f(\d+)", fetch_most_recent_file_name()).group(1))
                    + 1
                )
                + ".json"
            )

        # Combine the responses directly into the final file
        with open(f"{os.curdir}/app/series_files/{f}", "wb") as f_out:
            f_out.write(gzip.decompress(response.content))
            f_out.write(gzip.decompress(response_a.content))

        # Check if there are more than 2 files in the directory, delete the oldest one
        files = glob.glob(os.path.join(os.curdir + "/app/series_files/", "f*.json"))
        if len(files) > 2:
            files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))
            os.remove(files[0])
        print("File downloaded and extracted successfully.")


# CHECK IF THE SERIE ATTRIBUTES ARE THE SAME
def check_serie_attributes(serie_details, db_serie_details):
    attributes_to_check = [
        "id",
        "original_name",
        "name",
        "created_by",
        "poster_path",
        "all_titles",
        "adult",
        "overview",
        "first_air_date",
        "last_air_date",
        "next_episode_to_air",
        "networks",
        "in_production",
        "seasons",
        "number_of_seasons",
        "number_of_episodes",
        "last_episode_to_air",
        "episode_run_time",
        "genres",
        "homepage",
        "spoken_languages",
        "production_companies",
        "production_countries",
        "status",
        "popularity",
        "backdrop_path",
        "origin_country",
        "original_language",
    ]

    for attr in attributes_to_check:
        if attr == "all_titles":
            continue
        # Check if the attribute it's different, False if it is
        if db_serie_details.get(attr) != serie_details.get(attr):
            return False
    return True


# FETCH SERIES IMAGES
def fetch_serie_image(serie_id, path):
    url = f"https://image.tmdb.org/t/p/w500/{path}"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        image_path = os.path.join(IMAGES_DIR, f"{serie_id}.jpg")
        with open(image_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=128):
                f.write(chunk)
        return image_path
    else:
        return None


# UPDATE ALL SERIES IN THE DATABASE
def update_all_db_series():
    series = fetch_db_series()
    for s in series:
        serie_details = fetch_serie_details(s["id"])
        fetch_db_collection().update_one(
            {"id": s["id"]},
            {
                "$set": {
                    "overview": serie_details["overview"],
                    "release_date": serie_details["release_date"],
                    "original_language": serie_details["original_language"],
                    "original_name": serie_details["original_name"],
                    "title": serie_details["title"],
                    "all_titles": fetch_all_titles(s["id"]),
                    "poster_path": serie_details["poster_path"],
                    "backdrop_path": serie_details["backdrop_path"],
                    "genres": serie_details["genres"],
                    "status": serie_details["status"],
                    "origin_country": serie_details["origin_country"],
                    "production_companies": serie_details["production_companies"],
                    "production_countries": serie_details["production_countries"],
                    "spoken_languages": serie_details["spoken_languages"],
                }
            },
        )
        print(
            f"Serie UPDATED \"{s['original_name']}\" with id {s['id']}\" in the database"
        )


# COMPARE THE FILE DATA WITH THE DATABASE DATA AND ADD THE MISSING DATA TO THE DATABASE
def compare_series_file_db_add_db():
    try:
        last_id = fetch_last_known_serie_id()
        print(f"Last known serie ID in the database: {last_id}")
    except Exception as e:
        print(f"Error fetching last known serie ID: {e}")
        last_id = None
    dl_recent_series_ids()

    # Open and iterate through JSON file
    try:
        if fetch_most_recent_file() is None:
            print(
                "No serie file has been downloaded, try checking back the API caller!"
            )
            return
        else:
            with open(fetch_most_recent_file(), "r", encoding="utf-8") as f:

                file_lines = f.readlines()
                if len(file_lines) != len(fetch_series_from_mongodb()):
                    print(
                        f" Number of series in the database: {len(fetch_series_from_mongodb())}"
                        +f"\n Number of series in the file: {len(file_lines)}"
                        +"\n PROCEEDING TO ADD NEW SERIES TO THE DATABASE\n"
                    )
                    c = fetch_db_collection()
                    if (
                        last_id
                    ):  # check the db the last id and compare with the file if there are any which could be added
                        print("Option 1:")
                        print(
                            "There are series in the database, processing only new series:\n"
                        )
                        for line in file_lines:
                            try:
                                l_s = json.loads(line)
                                if l_s["id"] > last_id:
                                    details = fetch_serie_details(l_s["id"])
                                    if details and isinstance(details['id'], int):
                                        db_existing_serie = c.find_one({"id": l_s["id"]})

                                        # if the serie is not in the database, add it
                                        if not db_existing_serie:
                                            c.insert_one(
                                                {
                                                    "id": l_s["id"],
                                                    "original_name": l_s["original_name"],
                                                    "name": details["name"] if details["name"] is not None else l_s["original_name"],
                                                    "created_by": details["created_by"] if details["created_by"] is not None else None,
                                                    "poster_path": details["poster_path"] if details["poster_path"] is not None else None,
                                                    "all_titles": fetch_all_titles(
                                                        l_s["id"]
                                                    ),
                                                    "adult": details["adult"] if details["adult"] is not None else None,
                                                    "overview": details["overview"] if details["overview"] is not None else None,
                                                    "first_air_date": details[
                                                        "first_air_date"
                                                    ] if details["first_air_date"] is not None else None,
                                                    "last_air_date": details[
                                                        "last_air_date"
                                                    ] if details["last_air_date"] is not None else None,
                                                    "next_episode_to_air": details[
                                                        "next_episode_to_air"
                                                    ] if details["next_episode_to_air"] is not None else None,
                                                    "networks": details["networks"] if details["networks"] is not None else None,
                                                    "in_production": details[
                                                        "in_production"
                                                    ] if details["in_production"] is not None else None,
                                                    "seasons": details["seasons"] if details["seasons"] is not None else None,
                                                    "number_of_seasons": details[
                                                        "number_of_seasons"
                                                    ] if details["number_of_seasons"] is not None else None,
                                                    "number_of_episodes": details[
                                                        "number_of_episodes"
                                                    ] if details["number_of_episodes"] is not None else None,
                                                    "last_episode_to_air": details[
                                                        "last_episode_to_air"
                                                    ] if details["last_episode_to_air"] is not None else None,
                                                    "episode_run_time": details["episode_run_time"] if details["episode_run_time"] is not None else None,
                                                    "genres": details["genres"] if details["genres"] is not None else None,
                                                    "homepage": details["homepage"] if details["homepage"] is not None else None,
                                                    "spoken_languages": details[
                                                        "spoken_languages"
                                                    ] if details["spoken_languages"] is not None else None,
                                                    "production_companies": details[
                                                        "production_companies"
                                                    ] if details["production_companies"] is not None else None,
                                                    "production_countries": details[
                                                        "production_countries"
                                                    ] if details["production_countries"] is not None else None,
                                                    "status": details["status"] if details["status"] is not None else None,
                                                    "popularity": details["popularity"] if details["popularity"] is not None else None,
                                                    "backdrop_path": details[
                                                        "backdrop_path"
                                                    ] if details["backdrop_path"] is not None else None,
                                                    "origin_country": details[
                                                        "origin_country"
                                                    ] if details["origin_country"] is not None else None,
                                                    "original_language": details[
                                                        "original_language"
                                                    ] if details["original_language"] is not None else None,
                                                }
                                            )
                                            print(
                                                f"Serie ADDED \"{l_s['original_name']}\" with id \"{l_s['id']}\" to the database"
                                            )

                                        # if the serie is in the db and the details does not exist anymore, delete it
                                        elif not details and db_existing_serie:
                                            c.delete_one({"id": l_s["id"]})
                                            print(
                                                f"Serie DELETED \"{l_s['original_name']}\" with id \"{l_s['id']}\" from the database"
                                            )
                                    else:
                                        print(
                                            f"Error fetching serie details for ID '{l_s['id']}': {details}"
                                        )
                            except json.JSONDecodeError as e:
                                print(f"Error processing serie: {e}")
                        f.close()
                    else:
                        print("Option 2:")
                        print(
                            "There are no series in the database, processing all series:\n"
                        )
                        # for each serie, compare with the data in the database if it exists, if not add it, if yes nothing
                        for line in file_lines:
                            try:
                                l_s = json.loads(line)
                                details = fetch_serie_details(l_s["id"])                               
                                if details and isinstance(details['id'], int):
                                    db_existing_serie = c.find_one({"id": l_s["id"]})
                                    if not db_existing_serie:
                                        c.insert_one(
                                            {
                                                    "id": l_s["id"],
                                                    "original_name": l_s["original_name"],
                                                    "name": details["name"] if details["name"] is not None else l_s["original_name"],
                                                    "created_by": details["created_by"] if details["created_by"] is not None else None,
                                                    "poster_path": details["poster_path"] if details["poster_path"] is not None else None,
                                                    "all_titles": fetch_all_titles(
                                                        l_s["id"]
                                                    ),
                                                    "adult": details["adult"] if details["adult"] is not None else None,
                                                    "overview": details["overview"] if details["overview"] is not None else None,
                                                    "first_air_date": details[
                                                        "first_air_date"
                                                    ] if details["first_air_date"] is not None else None,
                                                    "last_air_date": details[
                                                        "last_air_date"
                                                    ] if details["last_air_date"] is not None else None,
                                                    "next_episode_to_air": details[
                                                        "next_episode_to_air"
                                                    ] if details["next_episode_to_air"] is not None else None,
                                                    "networks": details["networks"] if details["networks"] is not None else None,
                                                    "in_production": details[
                                                        "in_production"
                                                    ] if details["in_production"] is not None else None,
                                                    "seasons": details["seasons"] if details["seasons"] is not None else None,
                                                    "number_of_seasons": details[
                                                        "number_of_seasons"
                                                    ] if details["number_of_seasons"] is not None else None,
                                                    "number_of_episodes": details[
                                                        "number_of_episodes"
                                                    ] if details["number_of_episodes"] is not None else None,
                                                    "last_episode_to_air": details[
                                                        "last_episode_to_air"
                                                    ] if details["last_episode_to_air"] is not None else None,
                                                    "episode_run_time": details["episode_run_time"] if details["episode_run_time"] is not None else None,
                                                    "genres": details["genres"] if details["genres"] is not None else None,
                                                    "homepage": details["homepage"] if details["homepage"] is not None else None,
                                                    "spoken_languages": details[
                                                        "spoken_languages"
                                                    ] if details["spoken_languages"] is not None else None,
                                                    "production_companies": details[
                                                        "production_companies"
                                                    ] if details["production_companies"] is not None else None,
                                                    "production_countries": details[
                                                        "production_countries"
                                                    ] if details["production_countries"] is not None else None,
                                                    "status": details["status"] if details["status"] is not None else None,
                                                    "popularity": details["popularity"] if details["popularity"] is not None else None,
                                                    "backdrop_path": details[
                                                        "backdrop_path"
                                                    ] if details["backdrop_path"] is not None else None,
                                                    "origin_country": details[
                                                        "origin_country"
                                                    ] if details["origin_country"] is not None else None,
                                                    "original_language": details[
                                                        "original_language"
                                                    ] if details["original_language"] is not None else None,
                                                }
                                        )
                                        print(
                                            f"Serie ADDED \"{l_s['original_name']}\" with id \"{l_s['id']}\" to the database"
                                        )
                                    if not details and db_existing_serie:
                                        c.delete_one({"id": l_s["id"]})
                                        print(
                                            f"Serie DELETED \"{l_s['original_name']}\" with id \"{l_s['id']}\" from the database"
                                        )
                                else:
                                    print(
                                        f"Error fetching serie details for ID '{l_s['id']}': {details}"
                                    )
                            except json.JSONDecodeError as e:
                                print(f"Error processing serie: {e}")
                else:
                    print(
                        f"\nNumber of series in the database: {len(fetch_series_from_mongodb())}\nNumber of series in the file: {len(file_lines)}\nNO NEW SERIES TO ADD TO THE DATABASE\n"
                    )
    except json.JSONDecodeError as e:
        print(f"Error processing: {e}")


# UPDATE THE DATABASE WITH THE MOST RECENT FILE
def series_update_db():
    db_data = fetch_db_collection().find({})
    for data in db_data:
        serie_details = fetch_serie_details(data["id"])
        db_serie_details = fetch_db_collection().find_one({"id": data["id"]})
        if check_serie_attributes(serie_details, db_serie_details) == False:
            i = 0
            update_data = {
                "id": data["id"],
                "original_name": serie_details["original_name"],
                "name": serie_details["name"] if serie_details["name"] is not None else None,
                "created_by": serie_details["created_by"] if serie_details["created_by"] is not None else None,
                "poster_path": serie_details["poster_path"] if serie_details["poster_path"] is not None else None,
                "all_titles": fetch_all_titles(data["id"]),
                "adult": serie_details["adult"] if serie_details["adult"] is not None else None,
                "overview": serie_details["overview"] if serie_details["overview"] is not None else None,
                "first_air_date": serie_details["first_air_date"] if serie_details["first_air_date"] is not None else None,
                "last_air_date": serie_details["last_air_date"] if serie_details["last_air_date"] is not None else None,
                "next_episode_to_air": serie_details["next_episode_to_air"] if serie_details["next_episode_to_air"] is not None else None,
                "networks": serie_details["networks"] if serie_details["networks"] is not None else None,
                "in_production": serie_details["in_production"] if serie_details["in_production"] is not None else None,
                "seasons": serie_details["seasons"] if serie_details["seasons"] is not None else None,
                "number_of_seasons": serie_details["number_of_seasons"] if serie_details["number_of_seasons"] is not None else None,
                "number_of_episodes": serie_details["number_of_episodes"] if serie_details["number_of_episodes"] is not None else None,
                "last_episode_to_air": serie_details["last_episode_to_air"] if serie_details["last_episode_to_air"] is not None else None,
                "episode_run_time": serie_details["episode_run_time"] if serie_details["episode_run_time"] is not None else None,
                "genres": serie_details["genres"] if serie_details["genres"] is not None else None,
                "homepage": serie_details["homepage"] if serie_details["homepage"] is not None else None,
                "spoken_languages": serie_details["spoken_languages"] if serie_details["spoken_languages"] is not None else None,
                "production_companies": serie_details["production_companies"] if serie_details["production_companies"] is not None else None,
                "production_countries": serie_details["production_countries"] if serie_details["production_countries"] is not None else None,
                "status": serie_details["status"] if serie_details["status"] is not None else None,
                "popularity": serie_details["popularity"] if serie_details["popularity"] is not None else None,
                "backdrop_path": serie_details["backdrop_path"] if serie_details["backdrop_path"] is not None else None,
                "origin_country": serie_details["origin_country"] if serie_details["origin_country"] is not None else None,
                "original_language": serie_details["original_language"] if serie_details["original_language"] is not None else None,
            }

            for key, value in update_data.items():
                if value is None:
                    i += 1

            if len(update_data) - i < 10:
                fetch_db_collection().delete_one({"id": data["id"]})
                print(f"Serie DELETED \"{data['original_name']}\" with id \"{data['id']}\" from the database")
            else:
                fetch_db_collection().update_one(
                    {"id": data["id"]},
                    {"$set": update_data},
                )
                print(f"Serie UPDATED \"{data['original_name']}\" with id \"{data['id']}\" in the database")


# DOWNLOAD SERIE IMAGES (NOT TESTED YET)
def dl_serie_images():
    series = fetch_db_series()
    for s in series:
        if s["poster_path"]:
            image_path = fetch_serie_image(s["id"], s["poster_path"])
            if image_path:
                fetch_db_collection().update_one(
                    {"id": s["id"]}, {"$set": {"poster_path": image_path}}
                )
                print(
                    f"Image downloaded for serie \"{s['original_name']}\" with id \"{s['id']}\""
                )
            else:
                print(
                    f"Error downloading image for serie \"{s['original_name']}\" with id \"{s['id']}\""
                )
