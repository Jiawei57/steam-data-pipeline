import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
import random

import httpx
import json
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends 
from sqlalchemy import create_engine, Column, String, Integer, TIMESTAMP, text, inspect, Index
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from functools import wraps
from dotenv import load_dotenv

# --- Configuration & Setup ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv() # Load environment variables from .env file for local development

# Load credentials from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
PROXY_URLS = os.getenv("PROXY_URLS") # Comma-separated list of proxy URLs

if not DATABASE_URL:
    logging.error("DATABASE_URL environment variable not set. Application cannot start.")
    raise ValueError("DATABASE_URL environment variable not set.")

# --- Global Caches & Clients ---

# A simple in-memory cache for the Twitch token
twitch_token_cache: Dict[str, Any] = {"token": None, "expires_at": datetime.utcnow()}

# A simple in-memory lock to prevent concurrent scraping tasks
is_scraping: bool = False

# --- Constants ---
BATCH_SIZE = int(os.getenv("SCRAPER_BATCH_SIZE", 100))
CONCURRENCY_LIMIT = int(os.getenv("SCRAPER_CONCURRENCY_LIMIT", 10))
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

# Use a single, reusable httpx client for performance
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# The httpx client will now be created without a default proxy.
http_client = httpx.AsyncClient(
    timeout=30.0, 
    follow_redirects=True, 
    headers=headers
)

# Parse the comma-separated proxy URLs into a list for rotation
proxy_list = [url.strip() for url in PROXY_URLS.split(',')] if PROXY_URLS else []
if proxy_list:
    logging.info(f"Loaded {len(proxy_list)} proxies for rotation.")

def get_random_proxy_config() -> Optional[Dict[str, str]]:
    """Selects a random proxy from the list and returns it in httpx format."""
    if not proxy_list:
        return None
    proxy_url = random.choice(proxy_list)
    return {"http://": proxy_url, "https://": proxy_url}

# --- Database Setup (SQLAlchemy ORM) ---

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class GamesMetadata(Base):
    __tablename__ = "games_metadata"
    app_id = Column(String, primary_key=True)
    name = Column(String, index=True)
    type = Column(String)
    release_date = Column(String)
    developer = Column(String)
    publisher = Column(String)
    genres = Column(String)
    tags = Column(String) # This column is not currently populated, can be removed or used later
    metadata_last_updated = Column(TIMESTAMP(timezone=True), server_default=text('CURRENT_TIMESTAMP'), onupdate=lambda: datetime.now(timezone.utc))

class GamesTimeseries(Base):
    __tablename__ = "games_timeseries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    app_id = Column(String) # The composite index below covers this
    timestamp = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    price = Column(String)
    discount_percent = Column(Integer)
    player_count = Column(Integer)
    streamer_count = Column(Integer)

    __table_args__ = (
        # Composite index for efficient querying of a game's history
        Index('ix_games_timeseries_app_id_timestamp', 'app_id', 'timestamp'),
    )

class ScrapingState(Base):
    """A simple key-value table to store the state of the scraping process."""
    __tablename__ = "scraping_state"
    key = Column(String, primary_key=True)
    value = Column(String)

# --- External API Services ---

def retry_on_error(max_retries: int = 3, base_delay: float = 1.0):
    """A decorator to retry a function on HTTP errors with exponential backoff."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    # Use a semaphore to limit concurrency
                    async with semaphore:
                        return await func(*args, **kwargs)
                except (httpx.HTTPStatusError, httpx.RequestError, httpx.ProxyError, json.JSONDecodeError) as e:
                    # Retry on server errors (5xx), rate limiting (429), temp blocks (403), or proxy auth issues (407)
                    retriable_statuses = {403, 407, 429}
                    if isinstance(e, httpx.HTTPStatusError) and (e.response.status_code >= 500 or e.response.status_code in retriable_statuses):
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logging.warning(f"Request failed with HTTP status {e.response.status_code}. Retrying in {delay:.2f} seconds... (Attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(delay)
                        else:
                            logging.error(f"Request failed after {max_retries} attempts. Giving up. Error: {e}")
                            return None
                    # Retry on network errors (including ProxyError) or JSON decoding errors
                    elif isinstance(e, (httpx.RequestError, json.JSONDecodeError)):
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logging.warning(f"Request failed due to a network/proxy issue ({type(e).__name__}). Retrying in {delay:.2f} seconds... (Attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(delay)
                        else:
                            logging.error(f"Request failed after {max_retries} attempts. Giving up. Error: {e}")
                            return None
                    else:
                        # For other client errors (like 404), don't retry, just log and fail.
                        logging.error(f"Unrecoverable client error. Not retrying. Error: {e}")
                        return None
            return None
        return wrapper
    return decorator

@retry_on_error()
async def http_get(url: str, **kwargs) -> httpx.Response:
    """A wrapper for httpx.get that includes retry logic."""
    response = await http_client.get(url, **kwargs)
    response.raise_for_status()
    return response

@retry_on_error()
async def get_twitch_token() -> Optional[str]:
    """Fetches a Twitch app access token, using a cache to avoid repeated requests."""
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        logging.warning("Twitch credentials not set. Skipping streamer count.")
        return None

    # Return cached token if it's still valid
    if twitch_token_cache["token"] and twitch_token_cache["expires_at"] > datetime.now(timezone.utc):
        return twitch_token_cache["token"]

    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    # Let the retry decorator handle exceptions
    response = await http_client.post(url, params=params)
    response.raise_for_status()
    data = response.json()
    token = data["access_token"]
    expires_in = data.get("expires_in", 3600) # Default to 1 hour
    
    # Cache the new token and its expiry time
    twitch_token_cache["token"] = token
    twitch_token_cache["expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=expires_in * 0.9) # Refresh before it expires
    
    logging.info("Successfully fetched and cached a new Twitch token.")
    return token

async def fetch_paginated_list(base_url: str, limit: int, selector: str, id_extractor) -> List[str]:
    """A generic function to scrape paginated lists from Steam."""
    all_app_ids = []
    page = 1
    while len(all_app_ids) < limit:
        # Steam search pages use a 'page' query parameter.
        url = f"{base_url}&page={page}"
        logging.info(f"Fetching page {page} from {url}")
        # This request goes to store.steampowered.com, so we use the proxy.
        response = await http_get(url, proxies=get_random_proxy_config())
        soup = BeautifulSoup(response.text, "html.parser")
        page_app_ids = [id_extractor(row) for row in soup.select(selector) if id_extractor(row)]
        if not page_app_ids:
            break  # Stop if a page has no results, this is not an error.
        all_app_ids.extend(page_app_ids)
        page += 1
        await asyncio.sleep(1)  # Be polite and wait a second between page loads
    return all_app_ids[:limit]

# This function no longer needs a retry decorator, as the inner http_get handles it.
async def fetch_all_app_ids() -> List[str]:
    """Fetches all App IDs from the official Steam API."""
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/" # This is a public API, no proxy needed.
    try:
        response = await http_get(url)
        data = response.json()
        # The API returns a list of {"appid": 123, "name": "Game Name"}
        apps = data.get("applist", {}).get("apps", [])
        app_ids = [str(app.get("appid")) for app in apps if app.get("appid")]
        logging.info(f"Successfully fetched {len(app_ids)} total app IDs from Steam.")
        return app_ids
    except Exception as e:
        logging.error(f"Failed to fetch all app IDs: {e}")
        return []

# This function no longer needs a retry decorator, as the inner fetch_paginated_list now uses a retrying http_get.
async def fetch_top_selling_ids(limit: int = 500) -> List[str]:
    """Fetches Top Selling game IDs from Steam search, with retry logic."""
    logging.info("Fetching Top Sellers list...")
    ids = await fetch_paginated_list(
        base_url="https://store.steampowered.com/search/?filter=topsellers",
        limit=limit,
        selector="a.search_result_row",
        id_extractor=lambda row: row.get("data-ds-appid")
    )
    if not ids:
        # This will now only happen after all retries have failed
        raise Exception("Failed to fetch top selling IDs after multiple retries.")
    return ids

# This function no longer needs a retry decorator, as the inner http_get handles it.
async def fetch_most_played_ids() -> List[str]:
    """Fetches Most Played game IDs from Steam charts, with retry logic."""
    logging.info("Fetching Most Played list...")
    url = "https://store.steampowered.com/charts/mostplayed"
    # This request goes to store.steampowered.com, so we use the proxy.
    response = await http_get(url, proxies=get_random_proxy_config())
    soup = BeautifulSoup(response.text, "html.parser")
    ids = [row.get("data-appid") for row in soup.select("tr.weeklytopsellers_TableRow_2-RN6") if row.get("data-appid")]
    if not ids:
        raise Exception("Failed to fetch most played IDs after multiple retries.")
    return ids

@retry_on_error()
async def fetch_game_details(app_id: str) -> Optional[Dict[str, Any]]: # Keep retry here as it's a self-contained task
    """Fetches detailed metadata for a single game from the Steam API."""
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}" # Internal store API, needs proxy.
    try:
        # This request goes to store.steampowered.com, so we use the proxy.
        response = await http_get(url, proxies=get_random_proxy_config())
        data = response.json().get(app_id, {})
        if data.get("success"):
            details = data.get("data", {})
            return {
                "app_id": app_id,
                "name": details.get("name"),
                "type": details.get("type"),
                "release_date": details.get("release_date", {}).get("date"),
                "developer": ", ".join(details.get("developers", [])),
                "publisher": ", ".join(details.get("publishers", [])),
                "genres": ", ".join([g["description"] for g in details.get("genres", [])]),
                # Also extract price info here to avoid a second API call
                "price_overview": details.get("price_overview", {
                    "final_formatted": "N/A",
                    "discount_percent": 0
                })
            }
    except Exception as e:
        logging.error(f"Failed to fetch details for app_id {app_id}: {e}")
    return None

def normalize_game_name(name: str) -> str:
    """Removes common symbols that interfere with API lookups."""
    return name.replace('™', '').replace('®', '').strip()

@retry_on_error()
async def fetch_timeseries_data(app_id: str, game_name: str, price_info: Dict, twitch_token: Optional[str]) -> Optional[Dict[str, Any]]:
    """Fetches dynamic, time-series data for a single game."""
    # 1. Fetch player count
    player_count = 0
    if STEAM_API_KEY:
        try:
            player_url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}&key={STEAM_API_KEY}" # Public API, no proxy.
            player_response = await http_get(player_url)
            player_data = player_response.json().get("response", {})
            player_count = player_data.get("player_count", 0)
        except Exception:
            logging.warning(f"Could not fetch player count for app_id {app_id}.")

    # 2. Fetch streamer count
    streamer_count = 0
    if twitch_token and game_name:
        try:
            normalized_name = normalize_game_name(game_name)
            stream_url = f"https://api.twitch.tv/helix/streams?game_name={normalized_name}" # Twitch API, no proxy.
            headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {twitch_token}"}
            stream_response = await http_get(stream_url, headers=headers)
            streamer_count = len(stream_response.json().get("data", []))
        except Exception:
            logging.warning(f"Could not fetch streamer count for game '{game_name}'.")

    # 3. Use the pre-fetched price information
    price = price_info.get("final_formatted", "N/A")
    discount = price_info.get("discount_percent", 0)

    return {
        "app_id": app_id,
        "timestamp": datetime.now(timezone.utc),
        "price": price,
        "discount_percent": discount,
        "player_count": player_count,
        "streamer_count": streamer_count,
    }

# --- Main Pipeline Logic ---

async def scrape_and_store_data():
    """The main pipeline function that orchestrates the entire scraping process, with graceful shutdown support."""
    global is_scraping
    if is_scraping:
        logging.warning("Scraping process is already running. Skipping new trigger.")
        return

    is_scraping = True
    logging.info("Starting data scraping process...")
    db = SessionLocal()
    try:
        # Ensure tables exist
        Base.metadata.create_all(bind=engine)

        # --- Smart Hybrid Strategy: Fetch a pool of valuable games ---
        top_selling_ids, most_played_ids = [], []
        try:
            logging.info("Fetching candidate games from Top Sellers and Most Played lists...")
            
            top_selling_ids_task = fetch_top_selling_ids(limit=500)
            most_played_ids_task = fetch_most_played_ids()

            results = await asyncio.gather(top_selling_ids_task, most_played_ids_task, return_exceptions=True)
            top_selling_ids = results[0] if not isinstance(results[0], Exception) else []
            most_played_ids = results[1] if not isinstance(results[1], Exception) else []
        except Exception as e:
            logging.critical(f"An unexpected error occurred while gathering initial game lists: {e}", exc_info=True)

        # Combine and deduplicate the lists to create a high-value pool of games
        candidate_app_ids = sorted(list(set(top_selling_ids + most_played_ids)))

        if not candidate_app_ids:
            logging.warning("Aborting scrape: could not fetch any candidate app IDs.")
            return
        
        twitch_token = await get_twitch_token()

        total_apps = len(candidate_app_ids)
        logging.info(f"Processing a high-value pool of {total_apps} games in batches of {BATCH_SIZE}.")

        # The resumability logic is no longer needed for this shorter task, but batching is still a good practice.
        for i in range(0, total_apps, BATCH_SIZE):
            batch_ids = candidate_app_ids[i:i + BATCH_SIZE]
            
            # Graceful shutdown check
            if shutdown_event.is_set():
                logging.info("Shutdown signal received. Stopping before processing the next batch.")
                break

            logging.info(f"Processing batch {i//BATCH_SIZE + 1}/{(total_apps + BATCH_SIZE - 1)//BATCH_SIZE} (apps {i+1}-{i+len(batch_ids)})...")

            # 1. Fetch and Upsert Metadata for the batch
            metadata_tasks = [fetch_game_details(app_id) for app_id in batch_ids]
            metadata_results = await asyncio.gather(*metadata_tasks)
            valid_metadata = [m for m in metadata_results if m and m.get("name")]

            if valid_metadata:
                # Prepare a clean list of dictionaries for the database, excluding the temporary 'price_overview' key.
                metadata_to_upsert = [{k: v for k, v in m.items() if k != 'price_overview'} for m in valid_metadata]
                
                stmt = pg_insert(GamesMetadata).values(metadata_to_upsert)
                update_stmt = stmt.on_conflict_do_update(
                    index_elements=['app_id'],
                    set_={col.name: getattr(stmt.excluded, col.name) for col in GamesMetadata.__table__.columns if col.name != 'app_id'}
                )
                db.execute(update_stmt)
                db.commit()
                logging.info(f"Upserted {len(valid_metadata)} metadata records for this batch.")

            # 2. Fetch and Insert Timeseries data for the batch
            apps_to_fetch = [(m["app_id"], m["name"], m.get("price_overview", {})) for m in valid_metadata if m.get("type") == "game" and m.get("name")]
            if apps_to_fetch:
                timeseries_tasks = [fetch_timeseries_data(app_id, name, price_info, twitch_token) for app_id, name, price_info in apps_to_fetch]
                timeseries_results = await asyncio.gather(*timeseries_tasks)
                valid_timeseries = [t for t in timeseries_results if t]

                if valid_timeseries:
                    db.bulk_insert_mappings(GamesTimeseries, valid_timeseries)
                    db.commit()
                    logging.info(f"Inserted {len(valid_timeseries)} timeseries records for this batch.")
            
            # IMPORTANT: Pause between batches to respect API rate limits
            logging.info("Pausing for 5 seconds before next batch for safety...")
            await asyncio.sleep(5)

    except Exception as e:
        logging.error(f"An error occurred during the scraping pipeline: {e}", exc_info=True)
        db.rollback()
    finally:
        is_scraping = False # Release the lock
        db.close()
        if shutdown_event.is_set():
            logging.info("Scraping process gracefully shut down.")
        else:
            logging.info("Scraping process finished.")

# --- FastAPI Application ---

app = FastAPI(
    title="Steam Data Pipeline",
    description="An automated pipeline to scrape all Steam game data and store it in a PostgreSQL database.",
    version="3.0.0", # Version bump for major architectural improvements
)

# --- Dependency for DB Session ---
def get_db():
    """Dependency to get a new database session for each request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/games", summary="Fetch a few sample game records", tags=["Data"])
def get_games(db: Session = Depends(get_db)):
    """
    A simple endpoint to retrieve the first 5 game metadata records
    from the database to verify that data exists.
    """
    games = db.query(GamesMetadata).limit(5).all()
    if not games:
        return {"message": "No game data found. Has the scraper been run yet?"}
    return games

@app.on_event("shutdown")
async def on_shutdown():
    """Close the httpx client gracefully on application shutdown."""
    await http_client.aclose()

@app.get("/", summary="Health Check", tags=["Status"])
async def root():
    """Provides a simple health check endpoint."""
    return {"status": "ok", "message": "Steam Scraper Service is running."}

@app.get("/trigger-scrape", summary="Trigger Data Scraping", tags=["Actions"])
async def trigger_scrape(background_tasks: BackgroundTasks):
    """
    Triggers the data scraping and storage process as a background task.
    This endpoint is called by the Render Cron Job.
    """
    background_tasks.add_task(scrape_and_store_data)
    return {"message": "Data scraping process has been triggered in the background."}

# --- Global Shutdown Event for Graceful Worker Shutdown ---
shutdown_event = asyncio.Event()
