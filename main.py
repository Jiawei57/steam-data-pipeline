import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlencode
import random

import httpx
import json
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends 
from sqlalchemy import create_engine, Column, String, Integer, TIMESTAMP, text, inspect, Index, Numeric
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from functools import wraps
from dotenv import load_dotenv

# --- Configuration & Setup ---

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv() # Load environment variables from .env file for local development

# Load credentials from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")

if not DATABASE_URL:
    logging.error("DATABASE_URL environment variable not set. Application cannot start.")
    raise ValueError("DATABASE_URL environment variable not set.")

# --- Global Caches & Clients ---

# A simple in-memory cache for the Twitch token
twitch_token_cache: Dict[str, Any] = {"token": None, "expires_at": datetime.utcnow()}

# --- Constants ---
BATCH_SIZE = int(os.getenv("SCRAPER_BATCH_SIZE", 100))
CONCURRENCY_LIMIT = int(os.getenv("SCRAPER_CONCURRENCY_LIMIT", 10))
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
RETRIABLE_STATUSES = {403, 407, 429, 500, 502, 503, 504}

# Use a single, reusable httpx client for performance
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

http_client = httpx.AsyncClient(
    timeout=30.0, 
    follow_redirects=True, 
    headers=headers
)

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
    price_numeric = Column(Numeric(10, 2)) # e.g., 19.99
    price_currency = Column(String(3)) # e.g., 'USD', 'EUR'
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

async def make_request_with_retry(method: str, url: str, use_proxy: bool = False, **kwargs) -> Optional[httpx.Response]:
    """Makes an HTTP request with retry logic, using a semaphore to limit concurrency."""
    max_retries = 3
    base_delay = 5.0 # Increased base delay to be more respectful of rate limits

    request_url = url
    # If proxy is needed and the key is available, construct the ScraperAPI URL
    if use_proxy and SCRAPERAPI_KEY:
        payload = {'api_key': SCRAPERAPI_KEY, 'url': url}
        request_url = 'http://api.scraperapi.com/?' + urlencode(payload)
        logging.debug(f"Using proxy for URL: {url}")
    elif use_proxy and not SCRAPERAPI_KEY:
        logging.error(f"Attempted to use proxy for {url}, but SCRAPERAPI_KEY is not set. Making a direct request.")

    for attempt in range(max_retries):
        try:
            async with semaphore:
                if method.upper() == 'GET':
                    response = await http_client.get(request_url, **kwargs)
                elif method.upper() == 'POST':
                    response = await http_client.post(request_url, **kwargs)
                else:
                    logging.error(f"Unsupported HTTP method: {method}")
                    return None
                response.raise_for_status()
                return response
        except (httpx.HTTPStatusError, httpx.RequestError, httpx.ProxyError, json.JSONDecodeError) as e:
            is_retriable_status = isinstance(e, httpx.HTTPStatusError) and e.response.status_code in RETRIABLE_STATUSES
            is_network_error = isinstance(e, (httpx.RequestError, json.JSONDecodeError))

            if is_retriable_status or is_network_error:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(f"Request {method} {url} failed with {type(e).__name__}. Retrying in {delay:.2f}s... (Attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Request {method} {url} failed after {max_retries} attempts. Giving up. Final error: {e}")
            else:
                logging.error(f"Unrecoverable client error for {method} {url}. Not retrying. Error: {e}")
                break # Exit loop for non-retriable errors
    return None

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
    # This request does not need a proxy.
    response = await make_request_with_retry('POST', url, params=params)
    if not response:
        raise Exception("http_post for Twitch token returned None after retries.")
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
        # This request goes to store.steampowered.com, so we use a proxy.        
        response = await make_request_with_retry('GET', url, use_proxy=True)
        if not response:
            logging.error(f"Failed to fetch page {page} after all retries. Aborting paginated fetch.")
            break # Stop fetching more pages if one fails completely
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
        response = await make_request_with_retry('GET', url)
        if not response:
            raise Exception("http_get returned None")
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
    # This request goes to store.steampowered.com, so we use a proxy.    
    response = await make_request_with_retry('GET', url, use_proxy=True)
    if not response:
        raise Exception("Failed to fetch most played page after multiple retries.")
    soup = BeautifulSoup(response.text, "html.parser")
    ids = [row.get("data-appid") for row in soup.select("tr.weeklytopsellers_TableRow_2-RN6") if row.get("data-appid")]
    if not ids:
        raise Exception("Failed to fetch most played IDs after multiple retries.")
    return ids

async def fetch_game_details(app_id: str) -> Optional[Dict[str, Any]]: # Keep retry here as it's a self-contained task
    """Fetches detailed metadata for a single game from the Steam API."""
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}" # Internal store API, needs proxy.
    try:
        # This request goes to store.steampowered.com, so we use a proxy.        
        response = await make_request_with_retry('GET', url, use_proxy=True)
        if not response:
            raise Exception("http_get returned None")
        data = response.json().get(app_id, {})
        if data.get("success"):
            details = data.get("data", {})
            return {
                "app_id": app_id,
                "name": details.get("name", "").strip(), # Add strip() to clean up names
                "type": details.get("type"),
                "release_date": details.get("release_date", {}).get("date"),
                "developer": ", ".join(details.get("developers", [])),
                "publisher": ", ".join(details.get("publishers", [])),
                "genres": ", ".join([g["description"] for g in details.get("genres", [])]),
                # Also extract price info here to avoid a second API call
                # The price_overview object contains raw numeric values, which is much better.
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

async def fetch_timeseries_data(app_id: str, game_name: str, price_info: Optional[Dict], twitch_token: Optional[str]) -> Optional[Dict[str, Any]]:
    """Fetches dynamic, time-series data for a single game."""
    # 1. Fetch player count
    player_count = 0
    if STEAM_API_KEY:
        try:
            player_url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}&key={STEAM_API_KEY}"
            player_response = await make_request_with_retry('GET', player_url)
            if not player_response:
                raise Exception("http_get for player count returned None")
            player_data = player_response.json().get("response", {})
            player_count = player_data.get("player_count", 0)
        except Exception:
            logging.warning(f"Could not fetch player count for app_id {app_id}.")

    # 2. Fetch streamer count
    streamer_count = 0
    if twitch_token and game_name:
        try:
            normalized_name = normalize_game_name(game_name)
            stream_url = "https://api.twitch.tv/helix/streams"
            headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {twitch_token}"}
            stream_response = await make_request_with_retry('GET', stream_url, headers=headers, params={"game_name": normalized_name})
            if not stream_response:
                raise Exception("http_get for streamer count returned None")
            streamer_count = len(stream_response.json().get("data", []))
        except Exception:
            logging.warning(f"Could not fetch streamer count for game '{game_name}'.")

    # 3. Use the pre-fetched price information
    price_numeric = None
    price_currency = None
    discount = 0
    if price_info:
        # 'initial' is the price in the smallest currency unit (e.g., cents)
        price_numeric = price_info.get("initial", 0) / 100.0
        price_currency = price_info.get("currency")
        discount = price_info.get("discount_percent", 0)

    return {
        "app_id": app_id,
        "timestamp": datetime.now(timezone.utc),
        "price_numeric": price_numeric,
        "price_currency": price_currency,
        "discount_percent": discount,
        "player_count": player_count,
        "streamer_count": streamer_count,
    }

# --- Main Pipeline Logic ---

async def scrape_and_store_data():
    """The main pipeline function that orchestrates the entire scraping process, with graceful shutdown support."""
    db = SessionLocal()
    try:
        # Ensure tables exist
        Base.metadata.create_all(bind=engine)

        # --- Database-backed lock to prevent concurrent runs ---
        # Check if a scrape is already in progress
        is_running_record = db.query(ScrapingState).filter(ScrapingState.key == 'is_scraping_active').first()
        if is_running_record and is_running_record.value == 'true':
            # Check if the lock is stale (e.g., from a previous crash)
            last_started_record = db.query(ScrapingState).filter(ScrapingState.key == 'last_started_utc').first()
            if last_started_record:
                last_started_time = datetime.fromisoformat(last_started_record.value)
                if datetime.now(timezone.utc) - last_started_time > timedelta(hours=2):
                    logging.warning("Found a stale lock older than 2 hours. Overriding and starting a new scrape.")
                else:
                    logging.warning("Scraping process is already running according to database lock. Skipping new trigger.")
                    return

        # Set the lock
        db.merge(ScrapingState(key='is_scraping_active', value='true'))
        db.merge(ScrapingState(key='last_started_utc', value=datetime.now(timezone.utc).isoformat()))
        db.commit()
        logging.info("Database lock acquired. Starting data scraping process...")
        # --- End of lock logic ---

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
            logging.info("Pausing for 10 seconds before next batch to respect API rate limits...")
            await asyncio.sleep(10)

    except Exception as e:
        logging.error(f"An error occurred during the scraping pipeline: {e}", exc_info=True)
        db.rollback()
    finally:
        # Release the lock in the database
        db.merge(ScrapingState(key='is_scraping_active', value='false'))
        db.commit()
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

# --- Global Shutdown Event & Handler ---
shutdown_event = asyncio.Event()

@app.on_event("shutdown")
async def on_shutdown():
    """Set the shutdown event and close the httpx client gracefully."""
    logging.info("Shutdown signal received. Preparing to close resources.")
    shutdown_event.set() # Signal the scraper to stop
    logging.info("Closing HTTP client...")
    await http_client.aclose()
