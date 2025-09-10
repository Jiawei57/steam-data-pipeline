import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, BackgroundTasks
from sqlalchemy import create_engine, Column, String, Integer, TIMESTAMP, text, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
    tags = Column(String)
    metadata_last_updated = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'), onupdate=datetime.utcnow)

class GamesTimeseries(Base):
    __tablename__ = "games_timeseries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    app_id = Column(String, index=True)
    timestamp = Column(TIMESTAMP, default=datetime.utcnow, index=True)
    price = Column(String)
    discount_percent = Column(Integer)
    player_count = Column(Integer)
    streamer_count = Column(Integer)

# --- External API Services ---

async def get_twitch_token() -> Optional[str]:
    """Fetches a Twitch app access token, using a cache to avoid repeated requests."""
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        logging.warning("Twitch credentials not set. Skipping streamer count.")
        return None

    # Return cached token if it's still valid
    if twitch_token_cache["token"] and twitch_token_cache["expires_at"] > datetime.utcnow():
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
        twitch_token_cache["expires_at"] = datetime.utcnow() + timedelta(seconds=expires_in * 0.9) # Refresh before it expires
        
        logging.info("Successfully fetched and cached a new Twitch token.")
        return token
    except httpx.HTTPStatusError as e:
        logging.error(f"Failed to get Twitch token: {e.response.status_code} - {e.response.text}")
        return None

async def fetch_top_100_app_ids() -> List[str]:
    """Fetches the App IDs of the top 100 selling games on Steam via web scraping."""
    url = "https://store.steampowered.com/search/?filter=topsellers"
    try:
        response = await http_client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        app_ids = [
            row.get("data-ds-appid")
            for row in soup.select("a.search_result_row")
            if row.get("data-ds-appid")
        ]
        return app_ids[:100] # Ensure we only get the top 100
    except Exception as e:
        logging.error(f"Failed to scrape top 100 app IDs: {e}")
        return []

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
            }
    except Exception as e:
        logging.error(f"Failed to fetch details for app_id {app_id}: {e}")
    return None

async def fetch_timeseries_data(app_id: str, game_name: str, twitch_token: Optional[str]) -> Optional[Dict[str, Any]]:
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
            stream_url = f"https://api.twitch.tv/helix/streams?game_name={game_name}"
            headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {twitch_token}"}
            stream_response = await http_client.get(stream_url, headers=headers)
            streamer_count = len(stream_response.json().get("data", []))
        except Exception:
            logging.warning(f"Could not fetch streamer count for game '{game_name}'.")

    # 3. Fetch price (simplified from game details endpoint)
    price = "N/A"
    discount = 0
    try:
        details_url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
        details_response = await http_client.get(details_url)
        details_data = details_response.json().get(app_id, {}).get("data", {})
        if price_overview := details_data.get("price_overview"):
            price = price_overview.get("final_formatted")
            discount = price_overview.get("discount_percent")
    except Exception:
        logging.warning(f"Could not fetch price for app_id {app_id}.")

    return {
        "app_id": app_id,
        "timestamp": datetime.utcnow(),
        "price": price,
        "discount_percent": discount,
        "player_count": player_count,
        "streamer_count": streamer_count,
    }

# --- Main Pipeline Logic ---

async def scrape_and_store_data():
    """The main pipeline function that orchestrates the entire scraping process."""
    logging.info("Starting data scraping process...")
    db = SessionLocal()
    try:
        # Ensure tables exist
        Base.metadata.create_all(bind=engine)

        # Fetch top 100 games and their metadata
        top_app_ids = await fetch_top_100_app_ids()
        if not top_app_ids:
            logging.warning("Aborting scrape: could not fetch top 100 app IDs.")
            return
        
        logging.info(f"Found {len(top_app_ids)} top selling games. Fetching details...")
        metadata_tasks = [fetch_game_details(app_id) for app_id in top_app_ids]
        metadata_results = await asyncio.gather(*metadata_tasks)
        valid_metadata = [m for m in metadata_results if m and m.get("name")]

        # Upsert metadata into PostgreSQL
        if valid_metadata:
            stmt = pg_insert(GamesMetadata).values(valid_metadata)
            update_stmt = stmt.on_conflict_do_update(
                index_elements=['app_id'],
                set_={col.name: getattr(stmt.excluded, col.name) for col in GamesMetadata.__table__.columns if col.name != 'app_id'}
            )
            db.execute(update_stmt)
            db.commit()
            logging.info(f"Upserted {len(valid_metadata)} game metadata records.")

        # Fetch timeseries data for all successfully identified games
        twitch_token = await get_twitch_token()
        apps_to_fetch = [(m["app_id"], m["name"]) for m in valid_metadata]
        
        logging.info(f"Fetching timeseries data for {len(apps_to_fetch)} games...")
        timeseries_tasks = [fetch_timeseries_data(app_id, name, twitch_token) for app_id, name in apps_to_fetch]
        timeseries_results = await asyncio.gather(*timeseries_tasks)
        valid_timeseries = [t for t in timeseries_results if t]

        # Insert timeseries data
        if valid_timeseries:
            db.bulk_insert_mappings(GamesTimeseries, valid_timeseries)
            db.commit()
            logging.info(f"Inserted {len(valid_timeseries)} timeseries data records.")

    except Exception as e:
        logging.error(f"An error occurred during the scraping pipeline: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()
        logging.info("Scraping process finished.")

# --- FastAPI Application ---

app = FastAPI(
    title="Steam Data Pipeline",
    description="An automated pipeline to scrape Steam data and store it in a PostgreSQL database.",
    version="2.0.0",
)

@app.on_event("shutdown")
async def shutdown_event():
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
