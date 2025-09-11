import asyncio
import logging
import signal
from main import scrape_and_store_data, is_scraping, shutdown_event

def handle_shutdown_signal(sig, frame):
    """Sets the shutdown event when a signal is received."""
    logging.warning(f"Received shutdown signal {sig}. Attempting graceful shutdown...")
    shutdown_event.set()

if __name__ == "__main__":
    # Configure logging for the runner script
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    if is_scraping:
        logging.warning("Runner script exiting: A scraping process is already marked as running.")
    else:
        logging.info("Background worker started. Running the main scraping task.")
        try:
            asyncio.run(scrape_and_store_data())
        except Exception as e:
            logging.critical(f"Scraping task failed with a critical error: {e}", exc_info=True)
        logging.info("Background worker has completed its main task loop.")