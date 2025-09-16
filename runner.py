import asyncio
import logging
import signal
import os
from datetime import datetime, timedelta, timezone
from main import scrape_and_store_data, shutdown_event
import random

def handle_shutdown_signal(sig, frame):
    """當收到信號時，設定關閉事件。"""
    logging.warning(f"收到關閉信號 {sig}。正在嘗試優雅地關閉...")
    shutdown_event.set()

async def main_loop():
    """背景工人的主迴圈，負責每日在特定時間運行爬取任務。"""
    # 從環境變數獲取目標運行小時（UTC），預設為 17:00 UTC。
    target_hour_utc = int(os.getenv("WORKER_RUN_HOUR_UTC", 17))

    while not shutdown_event.is_set():
        logging.info("背景工人已啟動。正在運行主爬取任務。")
        task_successful = False
        try:
            await scrape_and_store_data()
            task_successful = True
            logging.info("爬取任務成功完成。")
        except Exception as e:
            logging.critical(f"爬取任務因嚴重錯誤而失敗: {e}", exc_info=True)
            logging.error("爬取任務因錯誤而終止。工人將休眠直到下一次排定的運行。")
        
        # 計算距離下一次排定運行的時間
        now_utc = datetime.now(timezone.utc)

        # --- 擬人化策略：為啟動時間加入隨機延遲 ---
        # 為啟動時間加入 0 到 120 分鐘的隨機偏移，以避免固定的模式。
        random_offset_minutes = random.randint(0, 120)
        
        # 今日的目標運行時間
        next_run_time = now_utc.replace(hour=target_hour_utc, minute=0, second=0, microsecond=0)
        
        # 如果今天的目標時間已過，則排定於明天
        if now_utc >= next_run_time:
            next_run_time += timedelta(days=1)
        
        # 加入隨機偏移。這可能會將時間推遲到現在之後，這是正常的。
        next_run_time += timedelta(minutes=random_offset_minutes)

        sleep_seconds = (next_run_time - now_utc).total_seconds()
        
        logging.info(f"工人將休眠 {sleep_seconds / 3600:.2f} 小時，直到下一次排定的運行時間: {next_run_time.isoformat()}。")
        
        # 等待休眠時間結束，每分鐘檢查一次關閉信號以便快速關閉。
        sleep_minutes = int(sleep_seconds // 60)
        remaining_seconds = sleep_seconds % 60

        for _ in range(sleep_minutes):
            if shutdown_event.is_set():
                break
            await asyncio.sleep(60)

        if not shutdown_event.is_set() and remaining_seconds > 0:
            await asyncio.sleep(remaining_seconds)

if __name__ == "__main__":
    # 為 runner 腳本設定日誌記錄
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 註冊信號處理器以實現優雅關閉
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    asyncio.run(main_loop())
    logging.info("工人迴圈已優雅地關閉。")