import os, sys, json, gzip, lzma, math, requests, tarfile, re
from pathlib import Path
from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import threading
from queue import Queue

# boot
sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in your .env")

DB_NAME = os.getenv("DB_NAME")
if not DB_NAME:
    raise RuntimeError("DB_NAME is not set in your .env")

COLLECTION = "musicbrainz_music"
DATA_DIR = Path("app/musicbrainz")
DATA_DIR.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist

DUMP_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/json-dumps"
COVER_ART_ARCHIVE_BASE_URL = "https://coverartarchive.org/release-group"

# Allowed release-group types
ALLOWED_TYPES = {"Album", "EP", "Single"}

# Configuration: Set to False to skip cover art fetching (faster processing)
FETCH_COVER_ART = True
# Configuration: If True, query API for each release (slow but verifies existence).
#                If False, construct direct URLs (fast, no API calls, but some may not exist)
USE_COVER_ART_API = False

# mongo
def fetch_db_collection():
    """Fetches and returns the MongoDB collection for music, creating index if needed"""
    mongo = MongoClient(MONGO_URI)
    collection = mongo[DB_NAME][COLLECTION]
    collection.create_index([("mbid", ASCENDING)], unique=True)
    return collection

# dump download and extraction
def get_latest_dump_date():
    """Get the latest dump date by reading the latest-is-* file"""
    try:
        response = requests.get(f"{DUMP_BASE_URL}/", timeout=30)
        if response.status_code == 200:
            html_content = response.text
            pattern = r'<a\s+href=["\'](latest-is-[^"\']+)["\']'
            matches = re.findall(pattern, html_content)
            
            if matches:
                latest_file = matches[0]
                if latest_file.startswith('latest-is-'):
                    dump_name = latest_file.replace('latest-is-', '').strip()
                    if dump_name and len(dump_name) >= 8:
                        return dump_name
        
        # Fallback: try LATEST file directly
        response = requests.get(f"{DUMP_BASE_URL}/LATEST", timeout=30)
        if response.status_code == 200:
            latest = response.text.strip()
            if latest and len(latest) >= 8:
                return latest
    except Exception as e:
        print(f"[WARNING] Could not fetch latest dump date: {e}")
    
    return None

def download_file(url: str, dest_path: Path, chunk_size: int = 8192):
    """Download a file with progress tracking"""
    print(f"[DOWNLOAD] Starting download: {url}")
    print(f"[DOWNLOAD] Destination: {dest_path}")
    
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    if downloaded % (chunk_size * 1000) == 0:  # Print every 1000 chunks
                        print(f"[DOWNLOAD] Progress: {percent:.2f}% ({downloaded}/{total_size} bytes)")
    
    print(f"[DOWNLOAD] Completed: {dest_path.name} ({downloaded} bytes)")

def extract_tar_xz(tar_path: Path, extract_to: Path = None):
    """Extract a .tar.xz file"""
    if extract_to is None:
        extract_to = tar_path.parent
    
    print(f"[EXTRACT] Extracting {tar_path.name} to {extract_to}")
    
    try:
        with tarfile.open(tar_path, 'r:xz') as tar:
            tar.extractall(extract_to)
        print(f"[EXTRACT] Completed: {tar_path.name}")
    except Exception as e:
        print(f"[ERROR] Failed to extract {tar_path.name}: {e}")
        raise

def download_and_extract_dump(dump_date: str = None):
    """Download and extract release-group dump from MusicBrainz"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    if dump_date is None or dump_date.upper() == "LATEST":
        dump_date = get_latest_dump_date()
        if not dump_date:
            print("[ERROR] Could not determine latest dump date. Please specify manually.")
            return None
        print(f"[INFO] Using latest dump date: {dump_date}")
    
    dump_dir = DATA_DIR / dump_date
    dump_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if already extracted
    # The file release-group (without extension) is directly in mbdump/
    mbdump_dir = dump_dir / "mbdump"
    release_group_file = mbdump_dir / "release-group"
    
    if release_group_file.exists() and release_group_file.is_file():
        print(f"[INFO] Dump already extracted at {release_group_file}")
        return dump_date
    
    # Download release-group.tar.xz
    tar_file = dump_dir / "release-group.tar.xz"
    if not tar_file.exists():
        url = f"{DUMP_BASE_URL}/{dump_date}/release-group.tar.xz"
        print(f"[INFO] Downloading release-group dump from {url}")
        try:
            download_file(url, tar_file)
        except Exception as e:
            print(f"[ERROR] Failed to download dump: {e}")
            return None
    else:
        print(f"[INFO] Tar file already exists: {tar_file}")
    
    # Extract the tar file
    if tar_file.exists():
        print(f"[INFO] Extracting {tar_file.name}...")
        try:
            extract_tar_xz(tar_file, dump_dir)
            # Verify extraction
            release_group_file = dump_dir / "mbdump" / "release-group"
            if release_group_file.exists():
                print(f"[INFO] Extraction complete. File is at {release_group_file}")
            else:
                print(f"[INFO] Extraction complete. Check {dump_dir / 'mbdump' / 'release-group'}")
            return dump_date
        except Exception as e:
            print(f"[ERROR] Failed to extract dump: {e}")
            return None
    
    return None

def get_cover_art_url(release_group_mbid: str, use_api: bool = False):
    """
    Get cover art URL for a release-group.
    
    Args:
        release_group_mbid: The MusicBrainz ID of the release-group
        use_api: If True, query Cover Art Archive API (slow but verifies existence).
                 If False, construct direct URL (fast, but may not exist for all releases).
    
    Returns:
        URL string to the cover art image, or None if not available
    """
    if use_api:
        # Query API - slower but verifies image exists
        try:
            url = f"{COVER_ART_ARCHIVE_BASE_URL}/{release_group_mbid}"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                # Get the front cover image (preferred) or first available image
                images = data.get('images', [])
                if images:
                    # Prefer front cover
                    for img in images:
                        if img.get('front', False):
                            return img.get('image')
                    # If no front cover, return first image
                    if images:
                        return images[0].get('image')
        except Exception:
            # Silently fail - cover art is optional
            pass
        return None
    else:
        # Direct URL construction - fast, no API call
        # Format: https://coverartarchive.org/release-group/{mbid}/front
        # Note: This URL may return 404 if cover doesn't exist, but that's fine
        # You can handle 404s when displaying images on your website
        return f"{COVER_ART_ARCHIVE_BASE_URL}/{release_group_mbid}/front"

def process_release_group_line(line: str, line_num: int, part: int, fetch_cover_art_flag: bool = True):
    """Process a single line from a release-group JSON file"""
    try:
        release_group = json.loads(line.strip())
        
        mbid = release_group.get('id') or release_group.get('mbid')
        if not mbid:
            return None, None, 0, 1  # operations, release_group_doc, processed, skipped
        
        # Filter by primary-type (Album, EP, Single only)
        primary_type = release_group.get('primary-type') or release_group.get('primary_type')
        if primary_type not in ALLOWED_TYPES:
            return None, None, 0, 1
        
        # Extract artist MBIDs and names from artist-credit
        artist_mbids = []
        artist_names = []
        if 'artist-credit' in release_group:
            for ac in release_group['artist-credit']:
                if 'artist' in ac:
                    artist = ac['artist']
                    artist_mbid = artist.get('id')
                    if artist_mbid:
                        artist_mbids.append(artist_mbid)
                        # Extract artist name
                        artist_name = artist.get('name')
                        if artist_name:
                            artist_names.append(artist_name)
        
        if not artist_mbids:
            return None, None, 0, 1
        
        # Extract tags
        tags = []
        if 'tags' in release_group:
            for tag in release_group.get('tags', []):
                if isinstance(tag, dict):
                    tags.append(tag.get('name'))
                elif isinstance(tag, str):
                    tags.append(tag)
        
        # Extract secondary types
        secondary_types = []
        if 'secondary-types' in release_group:
            for st in release_group.get('secondary-types', []):
                if isinstance(st, dict):
                    secondary_types.append(st.get('name'))
                elif isinstance(st, str):
                    secondary_types.append(st)
        elif 'secondary_types' in release_group:
            secondary_types = release_group.get('secondary_types', [])
        
        # Get cover art URL (optional)
        poster_url = None
        if fetch_cover_art_flag and FETCH_COVER_ART:
            poster_url = get_cover_art_url(mbid, use_api=USE_COVER_ART_API)
        
        # Create release-group document
        release_group_doc = {
            "mbid": mbid,
            "title": release_group.get('title'),
            "primary_type": primary_type,
            "secondary_types": secondary_types,
            "first_release_date": release_group.get('first-release-date') or release_group.get('first_release_date'),
            "tags": tags,
            "singers": artist_names if artist_names else None,
            "poster_url": poster_url,
            "_last_sync_at": datetime.utcnow()
        }
        
        # Create operations for each artist
        operations = []
        for artist_mbid in artist_mbids:
            operations.append(UpdateOne(
                {"mbid": artist_mbid},
                {
                    "$setOnInsert": {"mbid": artist_mbid},
                    "$addToSet": {"release_groups": release_group_doc}
                },
                upsert=True
            ))
        
        return operations, release_group_doc, 1, 0
        
    except json.JSONDecodeError as e:
        print(f"[Part {part}] [WARNING] Failed to parse line {line_num}: {e}")
        return None, None, 0, 1
    except Exception as e:
        print(f"[Part {part}] [WARNING] Error processing line {line_num}: {e}")
        return None, None, 0, 1

def worker_thread(queue: Queue, part: int, file_name: str, fetch_cover_art_flag: bool = True):
    """Worker thread that processes lines from the queue"""
    c = fetch_db_collection()
    
    operations = []
    processed = 0
    filtered_count = 0
    skipped = 0
    line_count = 0
    
    while True:
        item = queue.get()
        if item is None:  # Sentinel value to stop
            break
        
        line_num, line = item
        line_count += 1
        
        ops, doc, proc, skip = process_release_group_line(line, line_num, part, fetch_cover_art_flag)
        
        if ops:
            operations.extend(ops)
            processed += proc
            filtered_count += proc
        else:
            skipped += skip
        
        # Bulk write every 1000 operations
        if len(operations) >= 1000:
            result = c.bulk_write(operations, ordered=False)
            print(f"[Part {part}] Processed {processed} release-groups, {filtered_count} filtered (line {line_num}, batch: {len(operations)})")
            operations = []
        
        queue.task_done()
    
    # Insert remaining batch
    if operations:
        result = c.bulk_write(operations, ordered=False)
    
    print(f"[Part {part}] Completed file {file_name}: {processed} processed, {filtered_count} filtered (Album/EP/Single), {skipped} skipped")
    return processed, filtered_count, skipped

def process_release_group_file_threaded(json_file: Path, parts: int, fetch_cover_art_flag: bool = True):
    """Process a release-group file using multiple threads that read from a shared queue"""
    print(f"[INFO] Processing file: {json_file.name} with {parts} threads")
    
    # Determine file type and open accordingly
    try:
        with open(json_file, 'rb') as peek_file:
            magic = peek_file.read(2)
        
        if magic == b'\x1f\x8b':
            file_handle = gzip.open(json_file, 'rt', encoding='utf-8')
        elif magic == b'\xfd7':
            file_handle = lzma.open(json_file, 'rt', encoding='utf-8')
        else:
            file_handle = open(json_file, 'r', encoding='utf-8')
    except Exception as e:
        if json_file.suffix == '.gz':
            file_handle = gzip.open(json_file, 'rt', encoding='utf-8')
        elif json_file.suffix == '.xz':
            file_handle = lzma.open(json_file, 'rt', encoding='utf-8')
        else:
            file_handle = open(json_file, 'r', encoding='utf-8')
    
    # Create queue and worker threads
    queue = Queue(maxsize=parts * 1000)  # Buffer for up to 1000 lines per thread
    
    threads = []
    results = {}
    
    # Start worker threads
    for part in range(1, parts + 1):
        thread = threading.Thread(
            target=lambda p=part: results.update({p: worker_thread(queue, p, json_file.name, fetch_cover_art_flag)}),
            daemon=False
        )
        thread.start()
        threads.append(thread)
    
    # Read file and feed lines to queue
    line_count = 0
    with file_handle as f:
        for line_num, line in enumerate(f):
            queue.put((line_num, line))
            line_count += 1
    
    # Send sentinel values to stop workers
    for _ in range(parts):
        queue.put(None)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Aggregate results
    total_processed = sum(result[0] for result in results.values())
    total_filtered = sum(result[1] for result in results.values())
    total_skipped = sum(result[2] for result in results.values())
    return total_processed, total_filtered, total_skipped

def sync_music_threaded(dump_date: str = None, parts: int = 4, auto_download: bool = True, fetch_cover_art: bool = True):
    """Threaded synchronization: reads release-groups from dump files, filters to Album/EP/Single, and updates MongoDB"""
    # Ensure DATA_DIR exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Find the latest dump if not specified
    if dump_date is None or dump_date.upper() == "LATEST":
        dump_dirs = sorted([d for d in DATA_DIR.iterdir() if d.is_dir() and d.name != "summary"], reverse=True)
        if not dump_dirs:
            if auto_download:
                print("[INFO] No dump directories found. Attempting to download latest dump...")
                dump_date = download_and_extract_dump("LATEST")
                if not dump_date:
                    print("[ERROR] Failed to download dump. Please download manually.")
                    print(f"[INFO] Expected directory structure: {DATA_DIR}/YYYYMMDD-HHMMSS/mbdump/")
                    return
            else:
                print("[ERROR] No dump directories found. Please run the dump importer first.")
                print(f"[INFO] Expected directory structure: {DATA_DIR}/YYYYMMDD-HHMMSS/mbdump/")
                return
        else:
            dump_date = dump_dirs[0].name
            print(f"[INFO] Using latest dump: {dump_date}")
    
    dump_dir = DATA_DIR / dump_date
    
    if not dump_dir.exists():
        print(f"[ERROR] Dump directory {dump_dir} does not exist.")
        print(f"[INFO] Please extract the dump first. Expected: {dump_dir}/mbdump/")
        return
    
    # Find release-group file
    mbdump_dir = dump_dir / "mbdump"
    release_group_file = mbdump_dir / "release-group"
    
    release_group_files = []
    
    if release_group_file.exists() and release_group_file.is_file():
        release_group_files = [release_group_file]
        print(f"[INFO] Found release-group file: {release_group_file}")
    elif mbdump_dir.exists():
        release_group_files = [f for f in mbdump_dir.iterdir() if f.is_file() and f.name == "release-group"]
        if release_group_files:
            print(f"[INFO] Found release-group file(s) in {mbdump_dir}")
        else:
            release_group_files = [f for f in mbdump_dir.iterdir() if f.is_file() and "release-group" in f.name.lower()]
            if release_group_files:
                print(f"[INFO] Found release-group related file(s) in {mbdump_dir}")
    else:
        release_group_files = list(dump_dir.glob("**/release-group")) + list(dump_dir.glob("**/mbdump-release-group*"))
        release_group_files = [f for f in release_group_files if f.is_file()]
    
    if not release_group_files:
        if auto_download:
            print(f"[INFO] No release-group files found. Attempting to download and extract dump...")
            dump_date = download_and_extract_dump(dump_date)
            if dump_date:
                dump_dir = DATA_DIR / dump_date
                mbdump_dir = dump_dir / "mbdump"
                release_group_file = mbdump_dir / "release-group"
                if release_group_file.exists() and release_group_file.is_file():
                    release_group_files = [release_group_file]
                elif mbdump_dir.exists():
                    release_group_files = [f for f in mbdump_dir.iterdir() if f.is_file() and f.name == "release-group"]
        
        if not release_group_files:
            print(f"[ERROR] No release-group file found in {dump_dir}")
            mbdump_dir = dump_dir / "mbdump"
            print(f"[DEBUG] Checked path: {mbdump_dir / 'release-group'}")
            if mbdump_dir.exists():
                files_in_dir = list(mbdump_dir.iterdir())
                print(f"[DEBUG] Files/dirs in mbdump directory: {[f.name for f in files_in_dir]}")
            print("[INFO] Make sure the dump has been extracted. Check: app/musicbrainz/{dump_date}/mbdump/release-group")
            return
    
    print(f"[INFO] Found {len(release_group_files)} release-group file(s)")
    print(f"[INFO] Using {parts} threads per file")
    print(f"[INFO] Filtering for: {', '.join(ALLOWED_TYPES)}")
    print(f"[INFO] Fetch cover art: {fetch_cover_art}")
    
    c = fetch_db_collection()
    db_len_before = c.count_documents({})
    print(f"[INFO] Number of artists in database before sync: {db_len_before}")
    
    # Process each file with threading
    total_processed = 0
    total_filtered = 0
    total_skipped = 0
    
    for json_file in release_group_files:
        processed, filtered, skipped = process_release_group_file_threaded(json_file, parts, fetch_cover_art)
        total_processed += processed
        total_filtered += filtered
        total_skipped += skipped
    
    db_len_after = c.count_documents({})
    print(f"[DONE] Total processed: {total_processed} release-groups")
    print(f"[DONE] Total filtered (Album/EP/Single): {total_filtered}")
    print(f"[DONE] Total skipped: {total_skipped}")
    print(f"[INFO] Database: {db_len_before} → {db_len_after} artists (+{db_len_after - db_len_before})")

# main
if __name__ == "__main__":
    sync_music_threaded(dump_date="LATEST", parts=4, auto_download=True, fetch_cover_art=True)
