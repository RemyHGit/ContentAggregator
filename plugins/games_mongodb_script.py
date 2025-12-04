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
    r = requests.post(COUNT_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    return int(data[0]["count"] if isinstance(data, list) else data["count"])

def fetch_batch(offset: int) -> list[dict]:
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

    r = requests.post(IGDB_GAMES_URL, headers=HEADERS, data=body, timeout=60)
    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(3)
        r = requests.post(IGDB_GAMES_URL, headers=HEADERS, data=body, timeout=60)
    r.raise_for_status()
    return r.json()

def upsert_batch(docs: list[dict]) -> None:
    if not docs:
        return
    now = datetime.utcnow()
    ops = []
    for d in docs:
        if "id" not in d:
            continue
        d["_last_full_sync_at"] = now
        ops.append(UpdateOne({"id": d["id"]}, {"$set": d}, upsert=True))
    if ops:
        res = col.bulk_write(ops, ordered=False)
        print(f"  ↳ upserted={getattr(res, 'upserted_count', 0)} modified={getattr(res, 'modified_count', 0)}")

def process_range(start: int, end: int, part: int) -> int:
    fetched = 0
    offset = start
    while offset < end:
        batch = fetch_batch(offset)
        if not batch:
            break
        upsert_batch(batch)
        fetched += len(batch)
        offset += BATCH_SIZE
        print(f"Games Part {part} (Thread {start}-{end}) progress: {fetched} (last offset {offset})")
    return fetched

def partitions(total: int, parts: int):
    size = math.ceil(total / parts)
    out = []
    for i in range(parts):
        start = i * size
        end = min(start + size, total)
        if start < end:
            out.append((start, end, i + 1))
    return out

def sync_all_games_threaded(parts: int = 4, max_workers: int | None = None):
    total = count_igdb_games()
    print(f"[INFO] API count: {total}")
    if total == 0:
        return

    ranges = partitions(total, parts)
    print(f"[INFO] Using {len(ranges)} partitions of ~{math.ceil(total/len(ranges))} rows each")

    fetched_total = 0
    with ThreadPoolExecutor(max_workers=max_workers or parts, thread_name_prefix="games") as pool:
        futures = {pool.submit(process_range, s, e, p): (s, e, p) for (s, e, p) in ranges}
        for fut in as_completed(futures):
            (s, e, p) = futures[fut]
            got = fut.result()
            fetched_total += got
            print(f"[DONE] Partition {p} ({s}-{e}) fetched {got} rows")

    print(f"[DONE] Total fetched this run: {fetched_total}/{total}")

# main
if __name__ == "__main__":
    sync_all_games_threaded()
