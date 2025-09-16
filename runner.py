import asyncio
import logging
import signal
import os
from datetime import datetime, timedelta, timezone
from main import scrape_and_store_data, shutdown_event
import random

def handle_shutdown_signal(sig, frame):
    """Sets the shutdown event when a signal is received."""
    logging.warning(f"Received shutdown signal {sig}. Attempting graceful shutdown...")
    shutdown_event.set()

async def main_loop():
    """The main loop for the background worker that runs the scrape task daily at a specific time."""
    # Get target run hour from environment variable, default to 1 AM UTC.
    target_hour_utc = int(os.getenv("WORKER_RUN_HOUR_UTC", 17))

    while not shutdown_event.is_set():
        logging.info("Background worker started. Running the main scraping task.")
        task_successful = False
        try:
            await scrape_and_store_data()
            task_successful = True
            logging.info("Scraping task completed successfully.")
        except Exception as e:
            logging.critical(f"Scraping task failed with a critical error: {e}", exc_info=True)
            logging.error("Scraping task terminated due to an error. The worker will sleep until the next scheduled run.")
        
        # Calculate time until the next scheduled run
        now_utc = datetime.now(timezone.utc)

        # --- HUMANIZATION: Add random delay to the start time ---
        # Add a random offset of 0 to 120 minutes to the start time to avoid a fixed pattern.
        random_offset_minutes = random.randint(0, 120)
        
        # Today's target run time
        next_run_time = now_utc.replace(hour=target_hour_utc, minute=0, second=0, microsecond=0)
        
        # If the target time for today has already passed, schedule for tomorrow
        if now_utc >= next_run_time:
            next_run_time += timedelta(days=1)
        
        # Add the random offset. This might push the time past now, which is fine.
        next_run_time += timedelta(minutes=random_offset_minutes)

        sleep_seconds = (next_run_time - now_utc).total_seconds()
        
        logging.info(f"Worker will sleep for {sleep_seconds / 3600:.2f} hours until the next scheduled run at {next_run_time.isoformat()}.")
        
        # Wait for the sleep duration, checking for shutdown signal every minute for a quick shutdown.
        sleep_minutes = int(sleep_seconds // 60)
        remaining_seconds = sleep_seconds % 60

        for _ in range(sleep_minutes):
            if shutdown_event.is_set():
                break
            await asyncio.sleep(60)

        if not shutdown_event.is_set() and remaining_seconds > 0:
            await asyncio.sleep(remaining_seconds)

if __name__ == "__main__":
    # Configure logging for the runner script
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    asyncio.run(main_loop())
    logging.info("Worker loop has been shut down gracefully.")