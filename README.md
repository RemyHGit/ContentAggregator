# TMDB MongoDB Fetcher
TMDB MongoDB Fetcher is a project to fetch all the movies and series from the famous website **"TMDB"** and store it to a MongoDB Database for further uses.

## How it works 
That project works as follow : getting public movies id ; which anyone can download, clean them, ask the TMDB API for getting more details (useful ones), download every poster of movies / series if that's what you want and put all of theses data in a MongoDB database.

All that thing is working with threads, so fetching movies and series is simultaneous.

## How to run it
Put that line in the terminal at the root of the project : 
```bash
pip install -r requirements.txt
```
And then you can run the project, **HOWEVER** you need to create a .env file with these variables : 
```python
TMDB_API_KEY = "[your TMDB api key]"
LOCAL_MONGO_URI = "mongodb://localhost:27017/"
TMDB_DB_NAME = "tmdb"
IMAGES_DIR = "images"
```

Then, run conj.py and it should works

## Future for that project
For the nexts weeks, I plan on automatize it with a more dedicated way, like airflow for example.

I plan for getting more data, like games for exemple.

Adjust the code so that you get what you want, like "i only want movies" ;  "okay, here we go:".

My goal for that project is to achieve a way on getting some data on anything from website legally and for everyone, hope it's going anywhere with that idea.