import os, sys, math, requests, time
import json
from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# boot
sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in your .env")

DB_NAME = os.getenv("DB_NAME")
if not DB_NAME:
    raise RuntimeError("DB_NAME is not set in your .env")

COLLECTION = "google_books"
GOOGLE_BOOKS_API_URL = "https://www.googleapis.com/books/v1/volumes"
MAX_RESULTS_PER_REQUEST = 40  # Maximum allowed by Google Books API
BATCH_SIZE = 40

# mongo
def fetch_db_collection():
    mongo = MongoClient(MONGO_URI)
    collection = mongo[DB_NAME][COLLECTION]
    collection.create_index([("id", ASCENDING)], unique=True)
    return collection

def fetch_db_books():
    c = fetch_db_collection()
    return c.find()

# api
def fetch_books_by_query(query: str, start_index: int = 0, max_results: int = MAX_RESULTS_PER_REQUEST):
    """Fetches books from Google Books API with pagination"""
    params = {
        "q": query,
        "startIndex": start_index,
        "maxResults": max_results,
        "langRestrict": "fr,en"  # Limit to French and English books
    }
    
    try:
        response = requests.get(GOOGLE_BOOKS_API_URL, params=params, timeout=30)
        if response.status_code == 429:  # Rate limit
            time.sleep(2)
            response = requests.get(GOOGLE_BOOKS_API_URL, params=params, timeout=30)
        
        response.raise_for_status()
        data = response.json()
        return data.get("items", []), data.get("totalItems", 0)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching books for '{query}': {e}")
        return [], 0

def fetch_book_details(book_id: str):
    """Fetches complete book details by its ID"""
    url = f"{GOOGLE_BOOKS_API_URL}/{book_id}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 429:
            time.sleep(2)
            response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            print(f"Error fetching book {book_id}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching book {book_id}: {e}")
        return None

# helpers
def fetch_last_known_book_id():
    """Fetches the last known book ID in the database"""
    c = fetch_db_collection()
    last_book = c.find_one(sort=[("_last_sync_at", -1)])
    if last_book:
        return last_book.get("id")
    return None

# doc
def book_doc(book_data: dict):
    """Creates a structured MongoDB document from Google Books API data"""
    volume_info = book_data.get("volumeInfo", {})
    sale_info = book_data.get("saleInfo", {})
    
    # Extract authors
    authors = volume_info.get("authors", [])
    
    # Extract categories/genres
    categories = volume_info.get("categories", [])
    
    # Extract identifiers (ISBN, etc.)
    identifiers = {}
    for identifier in volume_info.get("industryIdentifiers", []):
        identifiers[identifier.get("type", "").lower()] = identifier.get("identifier", "")
    
    # Extract images
    image_links = volume_info.get("imageLinks", {})
    
    return {
        "id": book_data.get("id"),
        "title": volume_info.get("title"),
        "subtitle": volume_info.get("subtitle"),
        "authors": authors,
        "publisher": volume_info.get("publisher"),
        "publishedDate": volume_info.get("publishedDate"),
        "description": volume_info.get("description"),
        "isbn_10": identifiers.get("isbn_10"),
        "isbn_13": identifiers.get("isbn_13"),
        "pageCount": volume_info.get("pageCount"),
        "categories": categories,
        "language": volume_info.get("language"),
        "previewLink": volume_info.get("previewLink"),
        "infoLink": volume_info.get("infoLink"),
        "canonicalVolumeLink": volume_info.get("canonicalVolumeLink"),
        "thumbnail": image_links.get("thumbnail"),
        "smallThumbnail": image_links.get("smallThumbnail"),
        "averageRating": volume_info.get("averageRating"),
        "ratingsCount": volume_info.get("ratingsCount"),
        "maturityRating": volume_info.get("maturityRating"),
        "allowAnonLogging": volume_info.get("allowAnonLogging"),
        "contentVersion": volume_info.get("contentVersion"),
        "printType": volume_info.get("printType"),
        "saleability": sale_info.get("saleability"),
        "isEbook": sale_info.get("isEbook"),
        "listPrice": sale_info.get("listPrice"),
        "retailPrice": sale_info.get("retailPrice"),
        "buyLink": sale_info.get("buyLink"),
        "_last_sync_at": datetime.utcnow()
    }

# partitions
def partitions(total: int, parts: int):
    """Divides a total into multiple partitions"""
    size = math.ceil(total / parts)
    out = []
    for i in range(parts):
        start = i * size
        end = min(start + size, total)
        if start < end:
            out.append((start, end, i + 1))
    return out

# queries
SEARCH_QUERIES = [
    "fiction",
    "non-fiction",
    "science fiction",
    "romance",
    "mystery",
    "thriller",
    "biography",
    "history",
    "science",
    "philosophy",
    "literature",
    "poetry",
    "drama",
    "comedy",
    "fantasy",
    "horror",
    "adventure",
    "travel",
    "cooking",
    "art",
    "music",
    "technology",
    "business",
    "economics",
    "psychology",
    "education",
    "religion",
    "sports",
    "health",
    "self-help"
]


def collect_all_book_ids(queries: list = None, max_books_per_query: int = 1000):
    """Collects all book IDs from different search queries"""
    if queries is None:
        queries = SEARCH_QUERIES
    
    all_book_ids = set()
    
    for query in queries:
        print(f"[INFO] Searching books for: '{query}'")
        start_index = 0
        books_found = 0
        
        while books_found < max_books_per_query:
            items, total_items = fetch_books_by_query(query, start_index, BATCH_SIZE)
            
            if not items:
                break
            
            for item in items:
                book_id = item.get("id")
                if book_id:
                    all_book_ids.add(book_id)
            
            books_found += len(items)
            print(f"  ↳ Found {len(items)} books for '{query}' (total: {books_found}, unique: {len(all_book_ids)})")
            
            if len(items) < BATCH_SIZE or start_index + BATCH_SIZE >= total_items:
                break
            
            start_index += BATCH_SIZE
            time.sleep(0.5)  # Rate limiting
    
    print(f"[INFO] Total of {len(all_book_ids)} unique book IDs collected")
    return list(all_book_ids)

# sync
def sync_books_threaded(parts: int = 4, max_books_per_query: int = 1000, queries: list = None):
    """Threaded synchronization of books from Google Books API"""
    c = fetch_db_collection()
    
    # Collect all book IDs
    print("[INFO] Collecting book IDs from Google Books API...")
    all_book_ids = collect_all_book_ids(queries, max_books_per_query)
    
    if not all_book_ids:
        print("[WARNING] No books found")
        return
    
    total_books = len(all_book_ids)
    db_len = c.count_documents({})
    print(f"[INFO] Number of books in the database: {db_len}")
    print(f"[INFO] Number of books to process: {total_books}")
    
    ranges = partitions(total_books, parts)
    print(f"[INFO] Using {len(ranges)} partitions to process {total_books} books")
    
    def worker(start: int, end: int, part: int) -> int:
        """Worker thread to process a partition of books"""
        upserted_count = 0
        book_ids_slice = all_book_ids[start:end]
        
        for idx, book_id in enumerate(book_ids_slice):
            try:
                book_data = fetch_book_details(book_id)
                if book_data and book_data.get("id"):
                    doc = book_doc(book_data)
                    c.update_one({"id": book_id}, {"$set": doc}, upsert=True)
                    print(
                        f"[Part {part}] Book UPSERTED \"{doc.get('title', book_id)}\" with id \"{book_id}\""
                    )
                    upserted_count += 1
                elif not book_data:
                    # Book deleted or not found
                    db_existing_book = c.find_one({"id": book_id})
                    if db_existing_book:
                        c.delete_one({"id": book_id})
                        print(f"[Part {part}] Book DELETED with id \"{book_id}\"")
            except Exception as e:
                print(f"[Part {part}] Error processing book {book_id}: {e}")
                continue
            
            # Rate limiting
            if (idx + 1) % 10 == 0:
                time.sleep(0.5)
        
        return upserted_count
    
    total_upserted = 0
    with ThreadPoolExecutor(max_workers=parts, thread_name_prefix="books") as pool:
        futures = {
            pool.submit(worker, s, e, p): (s, e, p) for (s, e, p) in ranges
        }
        for fut in as_completed(futures):
            s, e, p = futures[fut]
            upserted = fut.result()
            total_upserted += upserted
            print(f"[DONE] Part {p} ({s}-{e}) upserted {upserted} books.")
    
    print(f"[DONE] Total books upserted in this run: {total_upserted}")

# main
if __name__ == "__main__":
    # You can adjust parts, max_books_per_query and queries as needed
    sync_books_threaded(parts=10, max_books_per_query=500)

