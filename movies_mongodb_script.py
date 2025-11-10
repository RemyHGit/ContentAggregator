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
LOCAL_MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
IMAGES_DIR = os.getenv("IMAGES_DIR")

headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_KEY}"}


# FETCH THE DATABASE COLLECTION
def fetch_db_collection():
    conn = os.getenv("MONGODB_URI", LOCAL_MONGO_URI)
    client = MongoClient(conn)
    db = client[DB_NAME]
    return db["tmdb_movies"]


# FETCH ALL MOVIES FROM THE DATABASE
def fetch_db_movies():
    c = fetch_db_collection()
    return c.find()


# FETCH THE MOST RECENT FILE
def fetch_most_recent_file():

    # Create the directory if it doesn't exist
    if not os.path.exists("app/movies_files"):
        os.makedirs("app/movies_files")
    
    # Get a list of all files in the directory with the specified pattern
    files = glob.glob(os.path.join(os.curdir, "app/movies_files", "f*.json"))

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
    if not os.path.exists("app/movies_files"):
        os.makedirs("app/movies_files")
    
    # Get a list of all files in the directory with the specified pattern
    files = glob.glob(os.path.join(os.path.abspath(os.curdir), "app/movies_files", "f*.json"))

    # If no files match the pattern, return None
    if not files:
        return None

    # Extract the number after the letter "f" from the file name and sort the files based on that number
    files.sort(key=lambda x: int(re.search(r"f(\d+)", x).group(1)))

    # Return the file name with the highest number after the letter "f" without the extension
    return os.path.splitext(os.path.basename(files[-1]))[0]


# FETCH MOVIES FROM MONGODB
def fetch_movies_from_mongodb():
    mongodb_uri = os.getenv("MONGODB_URI", LOCAL_MONGO_URI)
    client = MongoClient(mongodb_uri)    
    db = client[DB_NAME]
    collection = db["movies"]

    movies_data = []
    for document in collection.find():
        movies_data.append(document)
    return movies_data


# FETCH MOVIES DETAILS
def fetch_movie_details(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}"
    response = requests.get(url, headers=headers)
    data = response.json()

    if response.status_code == 200:
        return data
    else:
        print(f"Error fetching movie details for ID {movie_id}: {response.status_code}")
        return None  # Handle error or return empty data


# FETCH ALL TITLES OF A MOVIE
def fetch_all_titles(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/alternative_titles?api_key={TMDB_API_KEY}"
    response = requests.get(url, headers=headers)

    # Check for response status code
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error fetching movie details for ID {movie_id}: {response.status_code}")
        return None  # Handle error or return empty data


# FETCH THE LAST KNOWN MOVIE ID IN THE DATABASE
def fetch_last_known_movie_id():
    c = fetch_db_collection()
    last_movie = c.find_one(sort=[("id", -1)])
    if last_movie:
        return last_movie["id"]
    else:
        return None


# DOWNLOAD THE MOST RECENT MOVIE AND ADULT MOVIE IDS FILE
def dl_recent_movie_ids():
    now = datetime.now()

    # Try to download the file for the current date
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


# CHECK IF THE MOVIE ATTRIBUTES ARE THE SAME
def check_movie_attributes(movie_details, db_movie_details):
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
        # Check if the attribute is different, False if it is
        if db_movie_details.get(attr) != movie_details.get(attr):
            return False
    return True


# FETCH MOVIES IMAGES
def fetch_movie_image(movie_id, path):
    url = f"https://image.tmdb.org/t/p/w500/{path}"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        image_path = os.path.join(IMAGES_DIR, f"{movie_id}.jpg")
        with open(image_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=128):
                f.write(chunk)
        return image_path
    else:
        return None


# UPDATE ALL MOVIES IN THE DATABASE
def update_all_db_movies():
    movies = fetch_db_movies()
    for m in movies:
        movie_details = fetch_movie_details(m["id"])
        fetch_db_collection().update_one(
            {"id": m["id"]},
            {
                "$set": {
                    "overview": movie_details["overview"],
                    "release_date": movie_details["release_date"],
                    "original_language": movie_details["original_language"],
                    "original_title": movie_details["original_title"],
                    "title": movie_details["title"],
                    "all_titles": fetch_all_titles(m["id"]),
                    "poster_path": movie_details["poster_path"],
                    "backdrop_path": movie_details["backdrop_path"],
                    "genres": movie_details["genres"],
                    "status": movie_details["status"],
                    "origin_country": movie_details["origin_country"],
                    "production_companies": movie_details["production_companies"],
                    "production_countries": movie_details["production_countries"],
                    "spoken_languages": movie_details["spoken_languages"],
                }
            },
        )
        print(
            f"Movie UPDATED \"{m['original_title']}\" with id {m['id']}\" in the database"
        )


# COMPARE THE FILE DATA WITH THE DATABASE DATA AND ADD THE MISSING DATA TO THE DATABASE
def compare_movies_file_db_add_db():
    try:
        last_id = fetch_last_known_movie_id()
        print(f"Last known movie ID in the database: {last_id}")
    except Exception as e:
        print(f"Error fetching last known movie ID: {e}")
        last_id = None
    dl_recent_movie_ids()

    # Open and iterate through JSON file
    try:
        if fetch_most_recent_file() is None:
            print(
                "No movie file has been downloaded, try checking back the API caller!"
            )
            return
        else:
            with open(fetch_most_recent_file(), "r", encoding="utf-8") as f:
                
                file_lines = f.readlines()
                if len(file_lines) != len(fetch_movies_from_mongodb()):
                    print(
                        f" Number of movies in the database: {len(fetch_movies_from_mongodb())}"
                        +f"\n Number of movies in the file: {len(file_lines)}"
                        +"\n PROCEEDING TO ADD NEW MOVIES TO THE DATABASE\n"
                    )
                    c = fetch_db_collection()
                    if (
                        last_id
                    ):  # check the db the last id and compare with the file if there are any which could be added
                        print("Option 1:")
                        print(
                            "There are movies in the database, processing only new movies:\n"
                        )
                        for line in file_lines:
                            try:
                                l_m = json.loads(line)
                                if l_m["id"] > last_id:
                                    details = fetch_movie_details(l_m["id"])
                                    if details and isinstance(details['id'], int):
                                        db_existing_movie = c.find_one({"id": l_m["id"]})
                                        
                                        # if the movie is not in the database, add it
                                        if not db_existing_movie:
                                            c.insert_one(
                                                {
                                                    "id": l_m["id"],
                                                    "original_title": l_m["original_title"],
                                                    "poster_path": details["poster_path"] if details['poster_path'] is not None else None,
                                                    "all_titles": fetch_all_titles(l_m["id"]),
                                                    "adult": l_m["adult"],
                                                    "overview": details["overview"] if details['overview'] is not None else None,
                                                    "release_date": details["release_date"] if details['release_date'] is not None else None,
                                                    "runtime": details["runtime"] if details['runtime'] is not None else None,
                                                    "genres": details["genres"] if details['genres'] is not None else None,
                                                    "spoken_languages": details[
                                                        "spoken_languages"
                                                    ] if details['spoken_languages'] is not None else None,
                                                    "production_companies": details[
                                                        "production_companies"
                                                    ] if details['production_companies'] is not None else None,
                                                    "production_countries": details[
                                                        "production_countries"
                                                    ] if details['production_countries'] is not None else None,
                                                    "status": details["status"] if details['status'] is not None else None,
                                                    "popularity": details["popularity"] if details['popularity'] is not None else None,
                                                    "video": details["video"] if details['video'] is not None else None,
                                                    "backdrop_path": details["backdrop_path"] if details['backdrop_path'] is not None else None,
                                                    "budget": details["budget"] if details['budget'] is not None else None,
                                                    "origin_country": details["origin_country"] if details['origin_country'] is not None else None,
                                                    "original_language": details[
                                                        "original_language"
                                                    ] if details['original_language'] is not None else None,
                                                }
                                            )
                                            print(
                                                f"Movie ADDED \"{l_m['original_title']}\" with id \"{l_m['id']}\" to the database"
                                            )
                                        
                                        # if the movie is in the db and the details does not exist anymore, delete it
                                        elif not details and db_existing_movie:
                                            c.delete_one({"id": l_m["id"]})
                                            print(
                                                f"Movie DELETED \"{l_m['original_title']}\" with id \"{l_m['id']}\" from the database"
                                            )
                            except json.JSONDecodeError as e:
                                print(f"Error processing movie: {e}")
                        f.close()
                    else:
                        print("Option 2:")
                        print(
                            "There are no movies in the database, processing all movies:\n"
                        )
                        # for each movie, compare with the data in the database if it exists, if not add it, if yes nothing
                        for line in file_lines:
                            try:
                                l_m = json.loads(line)
                                details = fetch_movie_details(l_m["id"])
                                if details and isinstance(details['id'], int):
                                    db_existing_movie = c.find_one({"id": l_m["id"]})
                                    if not db_existing_movie:
                                        c.insert_one(
                                            {
                                                "id": l_m["id"],
                                                "original_title": l_m["original_title"],
                                                "poster_path": details["poster_path"] if details['poster_path'] is not None else None,
                                                "all_titles": fetch_all_titles(l_m["id"]),
                                                "adult": l_m["adult"],
                                                "overview": details["overview"] if details['overview'] is not None else None,
                                                "release_date": details["release_date"] if details['release_date'] is not None else None,
                                                "runtime": details["runtime"] if details['runtime'] is not None else None,
                                                "genres": details["genres"] if details['genres'] is not None else None,
                                                "spoken_languages": details[
                                                    "spoken_languages"
                                                ] if details['spoken_languages'] is not None else None,
                                                "production_companies": details[
                                                    "production_companies"
                                                ] if details['production_companies'] is not None else None,
                                                "production_countries": details[
                                                    "production_countries"
                                                ] if details['production_countries'] is not None else None,
                                                "status": details["status"] if details['status'] is not None else None,
                                                "popularity": details["popularity"] if details['popularity'] is not None else None,
                                                "video": details["video"] if details['video'] is not None else None,
                                                "backdrop_path": details["backdrop_path"] if details['backdrop_path'] is not None else None,
                                                "budget": details["budget"] if details['budget'] is not None else None,
                                                "origin_country": details["origin_country"] if details['origin_country'] is not None else None,
                                                "original_language": details[
                                                    "original_language"
                                                ] if details['original_language'] is not None else None,
                                            }
                                        )
                                        print(
                                            f"Movie ADDED \"{l_m['original_title']}\" with id \"{l_m['id']}\" to the database"
                                        )
                                    if not details and db_existing_movie:
                                        c.delete_one({"id": l_m["id"]})
                                        print(
                                            f"Movie DELETED \"{l_m['original_title']}\" with id \"{l_m['id']}\" from the database"
                                        )
                            except json.JSONDecodeError as e:
                                print(f"Error processing movie: {e}")
                else:
                    print(
                        f"\nNumber of movies in the database: {len(fetch_movies_from_mongodb())}\nNumber of movies in the file: {len(file_lines)}\nNO NEW MOVIES TO ADD TO THE DATABASE\n"
                    )
    except json.JSONDecodeError as e:
        print(f"Error processing: {e}")


# UPDATE THE DATABASE WITH THE DISTANT DATA
def movies_update_db():
    db_data = fetch_db_collection().find({})
    for data in db_data:
        movie_details = fetch_movie_details(data["id"])
        db_movie_details = fetch_db_collection().find_one({"id": data["id"]})
        if check_movie_attributes(movie_details, db_movie_details) == False:
            i = 0
            update_data = {
                    "id": data["id"],
                    "original_title": movie_details["original_title"],
                    "poster_path": movie_details["poster_path"] if movie_details['poster_path'] is not None else None,
                    "all_titles": fetch_all_titles(data["id"]),
                    "adult": movie_details["adult"],
                    "overview": movie_details["overview"] if movie_details['overview'] is not None else None,
                    "release_date": movie_details["release_date"] if movie_details['release_date'] is not None else None,
                    "runtime": movie_details["runtime"] if movie_details['runtime'] is not None else None,
                    "genres": movie_details["genres"] if movie_details['genres'] is not None else None,
                    "spoken_languages": movie_details["spoken_languages"] if movie_details['spoken_languages'] is not None else None,
                    "production_companies": movie_details["production_companies"] if movie_details['production_companies'] is not None else None,
                    "production_countries": movie_details["production_countries"] if movie_details['production_countries'] is not None else None,
                    "status": movie_details["status"] if movie_details['status'] is not None else None,
                    "popularity": movie_details["popularity"] if movie_details['popularity'] is not None else None,
                    "video": movie_details["video"] if movie_details['video'] is not None else None,
                    "backdrop_path": movie_details["backdrop_path"] if movie_details['backdrop_path'] is not None else None,
                    "budget": movie_details["budget"] if movie_details['budget'] is not None else None,
                    "origin_country": movie_details["origin_country"] if movie_details['origin_country'] is not None else None,
                    "original_language": movie_details["original_language"] if movie_details['original_language'] is not None else None,
                }
            
            # For each attr, increment i if the value is None
            for key, value in update_data.items():
                if value is None:
                    i += 1

            # If i is greater than 10, delete the movie from the database
            if len(update_data) - i < 10:
                fetch_db_collection().delete_one({"id": data["id"]})
                print(f"Movie DELETED \"{data['name']}\" with id \"{data['id']}\" from the database")
            else:
                fetch_db_collection().update_one(
                    {"id": data["id"]},
                    {"$set": update_data},
                )
                print(f"Movie UPDATED \"{data['original_title']}\" with id \"{data['id']}\" in the database")


# DOWNLOAD MOVIE IMAGES (NOT TESTED YET)
def dl_movie_images():
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
