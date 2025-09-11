import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

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
http_client = httpx.AsyncClient(timeout=20.0)

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
                except (httpx.HTTPStatusError, httpx.RequestError, json.JSONDecodeError) as e:
                    # Retry on server errors (5xx) or rate limiting (429)
                    if isinstance(e, httpx.HTTPStatusError) and (e.response.status_code >= 500 or e.response.status_code == 429):
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logging.warning(f"Request failed with {e.response.status_code}. Retrying in {delay:.2f} seconds... (Attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(delay)
                        else:
                            logging.error(f"Request failed after {max_retries} attempts. Giving up. Error: {e}")
                            return None
                    # Retry on network errors or JSON decoding errors
                    elif isinstance(e, (httpx.RequestError, json.JSONDecodeError)):
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logging.warning(f"Request failed with {type(e).__name__}. Retrying in {delay:.2f} seconds... (Attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(delay)
                        else:
                            logging.error(f"Request failed after {max_retries} attempts. Giving up. Error: {e}")
                            return None
                    else:
                        # For other client errors (like 403, 404), don't retry, just log and fail.
                        logging.error(f"Unrecoverable client error. Not retrying. Error: {e}")
                        return None
            return None
        return wrapper
    return decorator

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
    try:
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
    except httpx.HTTPStatusError as e:
        logging.error(f"Failed to get Twitch token: {e.response.status_code} - {e.response.text}")
        return None

async def fetch_top_selling_app_ids(limit: int = 500) -> List[str]:
    """Fetches the App IDs of the top selling games on Steam via web scraping."""
    url = "https://store.steampowered.com/search/?filter=topsellers"
    try:
        response = await http_client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        app_ids = [
            row.get("data-ds-appid") for row in soup.select("a.search_result_row") if row.get("data-ds-appid")
        ]
        return app_ids[:limit]
    except Exception as e:
        logging.error(f"Failed to scrape top selling app IDs: {e}")
        return []

@retry_on_error()
async def fetch_all_app_ids() -> List[str]:
    """Fetches all App IDs from the official Steam API."""
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    try:
        response = await http_client.get(url)
        response.raise_for_status()
        data = response.json()
        # The API returns a list of {"appid": 123, "name": "Game Name"}
        apps = data.get("applist", {}).get("apps", [])
        app_ids = [str(app.get("appid")) for app in apps if app.get("appid")]
        logging.info(f"Successfully fetched {len(app_ids)} total app IDs from Steam.")
        return app_ids
    except Exception as e:
        logging.error(f"Failed to fetch all app IDs: {e}")
        return []

async def fetch_most_played_app_ids(limit: int = 500) -> List[str]:
    """Fetches the App IDs of the most played games on Steam via web scraping."""
    url = "https://store.steampowered.com/stats/Steam-Game-and-Player-Statistics-Updated-Daily"
    try:
        response = await http_client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        # The app ID is in the href of the link in the first column of each row
        app_ids = [row.select_one("a.gameLink")['href'].split('/app/')[1] for row in soup.select("tr.player_count_row") if row.select_one("a.gameLink")]
        return app_ids[:limit]
    except Exception as e:
        logging.error(f"Failed to scrape most played app IDs: {e}")
        return []

@retry_on_error()
async def fetch_game_details(app_id: str) -> Optional[Dict[str, Any]]:
    """Fetches detailed metadata for a single game from the Steam API."""
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    try:
        response = await http_client.get(url)
        response.raise_for_status()
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
            player_url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}&key={STEAM_API_KEY}"
            player_response = await http_client.get(player_url)
            player_data = player_response.json().get("response", {})
            player_count = player_data.get("player_count", 0)
        except Exception:
            logging.warning(f"Could not fetch player count for app_id {app_id}.")

    # 2. Fetch streamer count
    streamer_count = 0
    if twitch_token and game_name:
        try:
            normalized_name = normalize_game_name(game_name)
            stream_url = f"https://api.twitch.tv/helix/streams?game_name={normalized_name}"
            headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {twitch_token}"}
            stream_response = await http_client.get(stream_url, headers=headers)
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
        logging.info("Fetching candidate games from Top Sellers and Most Played lists...")
        top_selling_ids = await fetch_top_selling_app_ids()
        most_played_ids = await fetch_most_played_app_ids()

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
