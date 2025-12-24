import os, sys, json, gzip, requests
from pathlib import Path
from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime
from dotenv import load_dotenv
import threading
from queue import Queue
import shutil

# Handle gzip.BadGzipFile for Python < 3.8
try:
    from gzip import BadGzipFile
except ImportError:
    # Python < 3.8 doesn't have BadGzipFile, use OSError instead
    BadGzipFile = OSError

# boot
sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in your .env")

DB_NAME = os.getenv("DB_NAME")
if not DB_NAME:
    raise RuntimeError("DB_NAME is not set in your .env")

COLLECTION = "openlibrary_books"
APP_DIR = "/opt/airflow/app"
DATA_DIR = Path(f"{APP_DIR}/openlibrary")
DATA_DIR.mkdir(parents=True, exist_ok=True)

OPEN_LIBRARY_DUMP_BASE_URL = "https://openlibrary.org/data"
DUMP_FILES = {
    "editions": "ol_dump_editions_latest.txt.gz",
    "works": "ol_dump_works_latest.txt.gz",
    "authors": "ol_dump_authors_latest.txt.gz"
}

# mongo
def fetch_db_collection():
    """Fetches and returns the MongoDB collection for books, creating index if needed"""
    mongo = MongoClient(MONGO_URI)
    collection = mongo[DB_NAME][COLLECTION]
    collection.create_index([("key", ASCENDING)], unique=True)
    return collection

# dump download
def verify_gzip_file(file_path: Path) -> bool:
    """Verify that a gzip file is complete and not corrupted"""
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            # Try to read a small amount to verify it's valid
            f.read(1024)
            # Try to seek to end to verify complete file
            f.seek(0, 2)  # Seek to end
        return True
    except (EOFError, OSError, BadGzipFile) as e:
        print(f"[VERIFY] File appears corrupted: {e}")
        return False

def download_file(url: str, dest_path: Path, chunk_size: int = 8192, max_retries: int = 3):
    """Download a file with progress tracking and retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"[DOWNLOAD] Starting download (attempt {attempt + 1}/{max_retries}): {url}")
            print(f"[DOWNLOAD] Destination: {dest_path}")
            
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Use a temporary file during download
            temp_path = dest_path.with_suffix(dest_path.suffix + '.tmp')
            
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            if downloaded % (chunk_size * 10000) == 0:  # Print every 10MB
                                print(f"[DOWNLOAD] Progress: {percent:.2f}% ({downloaded}/{total_size} bytes)")
            
            # Verify file size matches expected
            if total_size > 0 and downloaded != total_size:
                print(f"[DOWNLOAD] Warning: Downloaded size ({downloaded}) doesn't match expected ({total_size})")
                if attempt < max_retries - 1:
                    temp_path.unlink()
                    continue
            
            # Move temp file to final location
            if dest_path.exists():
                dest_path.unlink()
            temp_path.rename(dest_path)
            
            # Verify gzip integrity
            print(f"[DOWNLOAD] Verifying gzip file integrity...")
            if not verify_gzip_file(dest_path):
                print(f"[DOWNLOAD] File verification failed. Will retry...")
                if attempt < max_retries - 1:
                    dest_path.unlink()
                    continue
                else:
                    raise ValueError("Downloaded file is corrupted and all retries exhausted")
            
            print(f"[DOWNLOAD] Completed: {dest_path.name} ({downloaded} bytes)")
            return True
            
        except Exception as e:
            print(f"[DOWNLOAD] Error on attempt {attempt + 1}: {e}")
            if dest_path.exists():
                dest_path.unlink()
            if attempt < max_retries - 1:
                print(f"[DOWNLOAD] Retrying...")
                continue
            else:
                raise
    
    return False

def cleanup_old_openlibrary_dumps(dump_type: str = "editions"):
    """Delete old OpenLibrary dump file before downloading new one"""
    if dump_type not in DUMP_FILES:
        return
    
    filename = DUMP_FILES[dump_type]
    dump_file = DATA_DIR / filename
    
    if dump_file.exists():
        try:
            # Calculate file size before deletion
            file_size = dump_file.stat().st_size
            dump_file.unlink()
            print(f"[CLEANUP] Deleted old dump file: {filename} ({file_size / (1024**3):.2f} GB)")
        except Exception as e:
            print(f"[CLEANUP] Error deleting old dump file {filename}: {e}")

def download_dump(dump_type: str = "editions", auto_download: bool = True, force_redownload: bool = False):
    """Download Open Library dump file"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    if dump_type not in DUMP_FILES:
        raise ValueError(f"Invalid dump type. Must be one of: {list(DUMP_FILES.keys())}")
    
    filename = DUMP_FILES[dump_type]
    dump_file = DATA_DIR / filename
    
    if dump_file.exists():
        print(f"[INFO] Dump file already exists: {dump_file}")
        
        # Verify file integrity
        print(f"[INFO] Verifying existing file integrity...")
        if not verify_gzip_file(dump_file):
            print(f"[WARNING] Existing file appears corrupted. Will re-download.")
            cleanup_old_openlibrary_dumps(dump_type)
        elif force_redownload:
            print(f"[INFO] Force re-download requested. Removing existing file.")
            cleanup_old_openlibrary_dumps(dump_type)
        else:
            return dump_file
    
    if not auto_download:
        print(f"[INFO] Auto-download disabled. Please download manually: {OPEN_LIBRARY_DUMP_BASE_URL}/{filename}")
        return None
    
    # Cleanup old dump before downloading new one
    cleanup_old_openlibrary_dumps(dump_type)
    
    url = f"{OPEN_LIBRARY_DUMP_BASE_URL}/{filename}"
    print(f"[INFO] Downloading {dump_type} dump from {url}")
    print(f"[WARNING] This file is very large (several GB). Download may take a long time.")
    
    try:
        download_file(url, dump_file)
        return dump_file
    except Exception as e:
        print(f"[ERROR] Failed to download dump: {e}")
        return None

def process_edition_line(line: str, line_num: int, part: int, debug: bool = False):
    """Process a single line from Open Library editions dump"""
    # Skip empty lines and comments (don't count as skipped)
    stripped_line = line.strip()
    if not stripped_line or stripped_line.startswith('#'):
        return None, None, 0, 0  # Not an error, just skip silently
    
    try:
        # Open Library dump format: type\tkey\trevision\tlast_modified\tJSON
        # Note: Some dumps may have timestamp, but current format has 5 parts
        parts = stripped_line.split('\t', 4)
        
        # Debug logging for first few failures
        if len(parts) < 5:
            if debug and line_num < 10:
                print(f"[Part {part}] [DEBUG] Line {line_num} has {len(parts)} parts (expected 5). First 100 chars: {stripped_line[:100]}")
            return None, None, 0, 1  # operations, book_doc, processed, skipped
        
        # Handle both 5-part format (current) and 6-part format (with timestamp)
        if len(parts) == 5:
            dump_type, key, revision, last_modified, json_data = parts
            timestamp = None  # No timestamp in current format
        else:
            # Legacy 6-part format with timestamp
            dump_type, timestamp, key, revision, last_modified, json_data = parts
        
        # Skip if key is empty or invalid
        if not key or not key.startswith('/books/'):
            if debug and line_num < 10:
                print(f"[Part {part}] [DEBUG] Line {line_num} has invalid key: {key}")
            return None, None, 0, 1
        
        # Parse JSON data
        try:
            book_data = json.loads(json_data)
        except json.JSONDecodeError as e:
            if debug and line_num < 10:
                print(f"[Part {part}] [DEBUG] Line {line_num} JSON decode error: {e}. JSON preview: {json_data[:200]}")
            raise
        
        # Extract useful fields
        book_doc = {
            "key": key,
            "type": dump_type,
            "revision": int(revision) if revision and revision.isdigit() else None,
            "last_modified": last_modified,
            "title": book_data.get("title"),
            "subtitle": book_data.get("subtitle"),
            "authors": book_data.get("authors", []),
            "works": book_data.get("works", []),
            "isbn_10": book_data.get("isbn_10", []),
            "isbn_13": book_data.get("isbn_13", []),
            "publishers": book_data.get("publishers", []),
            "publish_date": book_data.get("publish_date"),
            "publish_places": book_data.get("publish_places", []),
            "number_of_pages": book_data.get("number_of_pages"),
            "languages": book_data.get("languages", []),
            "subjects": book_data.get("subjects", []),
            "subject_places": book_data.get("subject_places", []),
            "subject_people": book_data.get("subject_people", []),
            "subject_times": book_data.get("subject_times", []),
            "description": book_data.get("description"),
            "notes": book_data.get("notes"),
            "covers": book_data.get("covers", []),
            "lc_classifications": book_data.get("lc_classifications", []),
            "ocaid": book_data.get("ocaid"),
            "full_data": book_data,  # Store full data for reference
            "_last_sync_at": datetime.utcnow()
        }
        
        # Create operation
        operation = UpdateOne(
            {"key": key},
            {"$set": book_doc},
            upsert=True
        )
        
        return [operation], book_doc, 1, 0
        
    except json.JSONDecodeError as e:
        # Only log first few errors to avoid spam
        if line_num < 10:
            print(f"[Part {part}] [WARNING] Failed to parse JSON at line {line_num}: {e}")
        return None, None, 0, 1
    except Exception as e:
        # Only log first few errors to avoid spam
        if line_num < 10:
            print(f"[Part {part}] [WARNING] Error processing line {line_num}: {e}")
        return None, None, 0, 1

def worker_thread(queue: Queue, part: int, file_name: str, only_new: bool = True):
    """Worker thread that processes lines from the queue"""
    c = fetch_db_collection()
    
    operations = []
    processed = 0
    skipped = 0
    first_line = True
    
    while True:
        item = queue.get()
        if item is None:  # Sentinel value to stop
            break
        
        line_num, line = item
        
        # Enable debug for first few lines of each thread
        debug = first_line and line_num < 10
        ops, doc, proc, skip = process_edition_line(line, line_num, part, debug=debug)
        
        # Skip if document already exists in DB (when only_new=True)
        if only_new and ops and doc:
            existing = c.find_one({"key": doc.get("key")})
            if existing:
                skipped += 1
                ops = None  # Skip this operation
        
        if first_line and ops:
            print(f"[Part {part}] Successfully processed first book at line {line_num}: key={doc.get('key') if doc else 'N/A'}")
            first_line = False
        
        if ops:
            operations.extend(ops)
            processed += proc
        else:
            skipped += skip
        
        # Bulk write every 1000 operations
        if len(operations) >= 1000:
            try:
                c.bulk_write(operations, ordered=False)
                print(f"[Part {part}] Processed {processed} books (line {line_num}, batch: {len(operations)})")
            except Exception as e:
                print(f"[Part {part}] [ERROR] Bulk write failed at line {line_num}: {e}")
                # Try to write operations one by one to identify problematic documents
                for op in operations:
                    try:
                        c.bulk_write([op], ordered=False)
                    except Exception as op_err:
                        print(f"[Part {part}] [ERROR] Failed to write operation: {op_err}")
            operations = []
        
        queue.task_done()
    
    # Insert remaining batch
    if operations:
        try:
            c.bulk_write(operations, ordered=False)
        except Exception as e:
            print(f"[Part {part}] [ERROR] Final bulk write failed: {e}")
    
    print(f"[Part {part}] Completed file {file_name}: {processed} processed, {skipped} skipped")
    return processed, skipped

def process_dump_file_threaded(dump_file: Path, parts: int = 4, only_new: bool = True):
    """Process an Open Library dump file using multiple threads"""
    print(f"[INFO] Processing file: {dump_file.name} with {parts} threads")
    print(f"[INFO] Only new books: {only_new}")
    
    # Diagnostic: Read and analyze first few lines to understand format
    print(f"[INFO] Analyzing file format...")
    sample_lines = []
    try:
        with gzip.open(dump_file, 'rt', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 5:  # Read first 5 non-empty lines
                    break
                stripped = line.strip()
                if stripped and not stripped.startswith('#'):
                    sample_lines.append((i, stripped))
                    parts_count = len(stripped.split('\t', 4))
                    print(f"[INFO] Sample line {i}: {parts_count} parts, first 150 chars: {stripped[:150]}")
    except Exception as e:
        print(f"[WARNING] Could not analyze file format: {e}")
    
    # Create queue and worker threads
    queue = Queue(maxsize=parts * 1000)
    
    threads = []
    results = {}
    
    # Start worker threads
    for part in range(1, parts + 1):
        thread = threading.Thread(
            target=lambda p=part: results.update({p: worker_thread(queue, p, dump_file.name, only_new)}),
            daemon=False
        )
        thread.start()
        threads.append(thread)
    
    # Read file and feed lines to queue
    line_count = 0
    try:
        with gzip.open(dump_file, 'rt', encoding='utf-8') as f:
            for line_num, line in enumerate(f):
                queue.put((line_num, line))
                line_count += 1
                # Progress indicator every 1M lines
                if line_count % 1000000 == 0:
                    print(f"[INFO] Read {line_count:,} lines so far...")
    except (EOFError, OSError, BadGzipFile) as e:
        print(f"[ERROR] Failed to read gzip file: {e}")
        print(f"[ERROR] File may be corrupted or incomplete. Please delete and re-download.")
        # Send sentinel values to stop workers
        for _ in range(parts):
            queue.put(None)
        # Wait for threads to finish
        for thread in threads:
            thread.join()
        raise ValueError(f"Corrupted gzip file: {e}")
    
    print(f"[INFO] Finished reading {line_count:,} total lines")
    
    # Send sentinel values to stop workers
    for _ in range(parts):
        queue.put(None)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Aggregate results
    total_processed = sum(result[0] for result in results.values())
    total_skipped = sum(result[1] for result in results.values())
    return total_processed, total_skipped

def sync_books_threaded(dump_type: str = "editions", parts: int = 4, auto_download: bool = True, only_new: bool = True):
    """Threaded synchronization: reads books from Open Library dump and updates MongoDB"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Download dump if needed
    dump_file = download_dump(dump_type, auto_download)
    
    if not dump_file or not dump_file.exists():
        print(f"[ERROR] Dump file not found: {dump_file}")
        print(f"[INFO] Please download manually from: {OPEN_LIBRARY_DUMP_BASE_URL}/{DUMP_FILES.get(dump_type, '')}")
        return
    
    print(f"[INFO] Using dump file: {dump_file}")
    print(f"[INFO] Using {parts} threads")
    print(f"[INFO] Only new books: {only_new}")
    
    c = fetch_db_collection()
    db_len_before = c.count_documents({})
    print(f"[INFO] Number of books in database before sync: {db_len_before}")
    
    # Process dump file with threading
    total_processed, total_skipped = process_dump_file_threaded(dump_file, parts, only_new)
    
    db_len_after = c.count_documents({})
    print(f"[DONE] Total processed: {total_processed} books")
    print(f"[DONE] Total skipped: {total_skipped}")
    print(f"[INFO] Database: {db_len_before} → {db_len_after} books (+{db_len_after - db_len_before})")

# main
if __name__ == "__main__":
    sync_books_threaded(dump_type="editions", parts=4, auto_download=True, only_new=True)

