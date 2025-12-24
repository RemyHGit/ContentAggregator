import os, sys, time, math, requests
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne, ASCENDING
from concurrent.futures import ThreadPoolExecutor, as_completed

# boot
sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

TWITCH_ID = os.getenv("TWITCH_ID")
if not TWITCH_ID:
    raise RuntimeError("TWITCH_ID is not set in your .env")

TWITCH_SECRET = os.getenv("TWITCH_SECRET")
if not TWITCH_SECRET:
    raise RuntimeError("TWITCH_SECRET is not set in your .env")

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in your .env")

DB_NAME = os.getenv("DB_NAME")
if not DB_NAME:
    raise RuntimeError("DB_NAME is not set in your .env")

COLLECTION = "igdb_games"
IGDB_GAMES_URL = "https://api.igdb.com/v4/games"
COUNT_URL = "https://api.igdb.com/v4/games/count"
BATCH_SIZE = 500

# auth
def igdb_headers():
    """Fetches and returns authentication headers for IGDB API using Twitch credentials"""
    r = requests.post(
        "https://id.twitch.tv/oauth2/token",
        params={"client_id": TWITCH_ID, "client_secret": TWITCH_SECRET, "grant_type": "client_credentials"},
        timeout=30,
    )
    r.raise_for_status()
    token = r.json()["access_token"]
    return {"Client-ID": TWITCH_ID, "Authorization": f"Bearer {token}"}

HEADERS = igdb_headers()

# mongo
mongo = MongoClient(MONGO_URI)
col = mongo[DB_NAME][COLLECTION]
col.create_index([("id", ASCENDING)], unique=True)

# helpers
def count_igdb_games() -> int:
    """Returns the total count of games available in IGDB API"""
    r = requests.post(COUNT_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    return int(data[0]["count"] if isinstance(data, list) else data["count"])

def fetch_batch(offset: int) -> list[dict]:
    """Fetches a batch of games from IGDB API starting at the given offset"""
    fields = (
        "id,name,cover.url,first_release_date,genres.name,"
        "involved_companies.company.name,platforms.name,summary,url,updated_at"
    )
    parts = []

    parts.append(f"fields {fields};")
    parts.append("sort id asc;")
    parts.append(f"limit {BATCH_SIZE};")
    parts.append(f"offset {offset};")
    body = " ".join(parts)

    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        r = requests.post(IGDB_GAMES_URL, headers=HEADERS, data=body, timeout=60)
        
        if r.status_code == 429:  # Rate limit
            wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
            print(f"  ↳ Rate limit (429) hit, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
            time.sleep(wait_time)
            continue
        elif r.status_code in (500, 502, 503, 504):  # Server errors
            wait_time = retry_delay * (attempt + 1)
            print(f"  ↳ Server error ({r.status_code}), waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
            time.sleep(wait_time)
            continue
        elif r.status_code == 200:
            return r.json()
        else:
            r.raise_for_status()
    
    # If we get here, all retries failed
    raise requests.exceptions.HTTPError(f"Failed after {max_retries} retries: {r.status_code}")

def convert_timestamp_to_date(timestamp: int) -> str:
    """Converts Unix timestamp to ISO format date string"""
    if not timestamp:
        return None
    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.isoformat()
    except (ValueError, OSError, TypeError):
        return None

def upsert_batch(docs: list[dict]) -> None:
    """Upserts a batch of game documents into MongoDB"""
    if not docs:
        return
    now = datetime.utcnow()
    ops = []
    for d in docs:
        if "id" not in d:
            continue
        # Convert first_release_date from timestamp to ISO date string
        if "first_release_date" in d and d["first_release_date"]:
            d["first_release_date"] = convert_timestamp_to_date(d["first_release_date"])
        d["_last_full_sync_at"] = now
        ops.append(UpdateOne({"id": d["id"]}, {"$set": d}, upsert=True))
    if ops:
        res = col.bulk_write(ops, ordered=False)
        print(f"  ↳ upserted={getattr(res, 'upserted_count', 0)} modified={getattr(res, 'modified_count', 0)}")

def process_range(start: int, end: int, part: int, only_new: bool = True) -> int:
    """Processes a range of game IDs, fetching and upserting them in batches"""
    fetched = 0
    offset = start
    empty_batches = 0
    max_empty_batches = 3  # Allow a few empty batches before stopping
    
    while offset < end:
        try:
            batch = fetch_batch(offset)
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                print(f"Games Part {part}: Rate limit exceeded, waiting 60s before continuing...")
                time.sleep(60)
                continue
            else:
                raise
        
        if not batch:
            empty_batches += 1
            if empty_batches >= max_empty_batches:
                print(f"Games Part {part} (Thread {start}-{end}): Stopping after {empty_batches} empty batches at offset {offset}")
                break
            # Skip this offset and continue
            offset += BATCH_SIZE
            time.sleep(0.5)  # Small delay between batches
            continue
        
        empty_batches = 0  # Reset counter on successful fetch
        
        # Filter out games that already exist if only_new=True
        if only_new:
            batch_ids = [g.get("id") for g in batch if g.get("id")]
            if batch_ids:
                existing_ids = set(col.find({"id": {"$in": batch_ids}}, {"id": 1}).distinct("id"))
                batch = [g for g in batch if g.get("id") not in existing_ids]
        
        if batch:  # Only upsert if there are new games
            upsert_batch(batch)
            fetched += len(batch)
        
        offset += BATCH_SIZE
        print(f"Games Part {part} (Thread {start}-{end}) progress: {fetched} (last offset {offset})")
        
        # Rate limiting: small delay between batches to avoid hitting limits
        time.sleep(0.2)
    
    return fetched

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

def sync_all_games_threaded(parts: int = 4, max_workers: int | None = None, only_new: bool = True):
    """Threaded synchronization of all games from IGDB API to MongoDB"""
    total = count_igdb_games()
    print(f"[INFO] API count: {total}")
    if total == 0:
        print("[WARNING] API returned 0 games, aborting")
        return

    # Check current database count
    db_count = col.count_documents({})
    print(f"[INFO] Current games in database: {db_count}")
    print(f"[INFO] Only new games: {only_new}")

    ranges = partitions(total, parts)
    print(f"[INFO] Using {len(ranges)} partitions of ~{math.ceil(total/len(ranges))} rows each")

    fetched_total = 0
    with ThreadPoolExecutor(max_workers=max_workers or parts, thread_name_prefix="games") as pool:
        futures = {pool.submit(process_range, s, e, p, only_new): (s, e, p) for (s, e, p) in ranges}
        for fut in as_completed(futures):
            (s, e, p) = futures[fut]
            got = fut.result()
            fetched_total += got
            expected = e - s
            if got < expected:
                print(f"[WARNING] Partition {p} ({s}-{e}) fetched {got} rows, expected ~{expected}")
            else:
                print(f"[DONE] Partition {p} ({s}-{e}) fetched {got} rows")

    print(f"[DONE] Total fetched this run: {fetched_total}/{total}")
    if fetched_total < total:
        print(f"[WARNING] Fetched {fetched_total} games but API reports {total} total. Some games may be inaccessible.")
    
    # Final database count
    final_db_count = col.count_documents({})
    print(f"[INFO] Final games in database: {final_db_count} (added {final_db_count - db_count} this run)")

# main
if __name__ == "__main__":
    sync_all_games_threaded(only_new=True)
