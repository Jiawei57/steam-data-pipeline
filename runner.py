import asyncio
import logging
import signal
import os
from datetime import datetime, timedelta, timezone
import subprocess
from main import scrape_and_store_data, shutdown_event
import random

def handle_shutdown_signal(sig, frame):
    """
    信號處理器，用於接收 SIGTERM 或 SIGINT 信號。
    當收到信號時，它會設定一個全域的 `shutdown_event`，
    通知主迴圈應該在完成當前任務後優雅地退出，而不是被強制中斷。
    """
    logging.warning(f"收到關閉信號 {sig}。正在嘗試優雅地關閉...")
    shutdown_event.set()

async def main_loop():
    """背景工人的主迴圈，負責每日在特定時間運行爬取任務。"""
    # 從環境變數獲取目標運行小時（UTC），預設為 17:00 UTC。
    target_hour_utc = int(os.getenv("WORKER_RUN_HOUR_UTC", 17))
    
    # 首次啟動時，檢查 commit 訊息
    is_first_run = True

    # 這是背景工人的主生命週期迴圈。只要沒有收到關閉信號，它就會一直運行。
    while not shutdown_event.is_set():
        should_run_now = True
        # 僅在服務首次啟動時檢查 commit 訊息
        if is_first_run:
            is_first_run = False # 確保此邏輯只執行一次
            try:
                # 執行 git 指令獲取最新的 commit 訊息
                result = subprocess.run(
                    ["git", "log", "-1", "--pretty=%B"],
                    capture_output=True, text=True, check=True, encoding='utf-8'
                )
                commit_message = result.stdout
                if "[skip-run]" in commit_message:
                    logging.info("偵測到 '[skip-run]' 提交訊息。將跳過本次啟動任務，直接進入排程等待。")
                    should_run_now = False
            except FileNotFoundError:
                logging.warning("無法執行 'git' 指令 (可能未安裝或不在 PATH 中)。將採用預設行為，立即運行任務。")
            except subprocess.CalledProcessError as e:
                # 在 Render/Docker 環境中，.git 目錄通常不存在，會導致 'exit status 128'。
                # 我們將此視為一種預期情況，並將日誌等級從 ERROR 降為 INFO。
                if e.returncode == 128:
                    logging.info(f"無法讀取 git 歷史記錄 (在部署環境中為正常現象)。將採用預設行為，立即運行任務。")
                else:
                    logging.error(f"獲取 git commit 訊息失敗: {e}。將採用預設行為，立即運行任務。")

        if should_run_now:
            logging.info("背景工人已啟動。正在運行主爬取任務。")
            try:
                await scrape_and_store_data()
                logging.info("爬取任務成功完成。")
            except Exception as e:
                logging.critical(f"爬取任務因嚴重錯誤而失敗: {e}", exc_info=True)
                logging.error("爬取任務因錯誤而終止。工人將休眠直到下一次排定的運行。")
        
        # 計算距離下一次排定運行的時間
        now_utc = datetime.now(timezone.utc)

        # --- 擬人化策略：為啟動時間加入隨機延遲 ---
        # 為啟動時間加入 0 到 120 分鐘的隨機偏移，這是一種反爬蟲與反追蹤策略。
        # 避免每日都在完全相同的時間點發出請求，讓行為模式更難被預測。
        random_offset_minutes = random.randint(0, 120)
        
        # 今日的目標運行時間
        next_run_time = now_utc.replace(hour=target_hour_utc, minute=0, second=0, microsecond=0)
        
        # 如果今天的目標時間已過，則排定於明天
        if now_utc >= next_run_time:
            next_run_time += timedelta(days=1)
        
        # 加入隨機偏移。這可能會將時間推遲到現在之後，這是正常的。
        next_run_time += timedelta(minutes=random_offset_minutes)

        sleep_seconds = (next_run_time - now_utc).total_seconds()
        
        # 確保休眠時間不為負數，以防萬一。
        if sleep_seconds <= 0:
            logging.warning("計算出的休眠時間小於等於零，將立即進入下一個週期。")
            continue

        logging.info(f"工人將休眠 {sleep_seconds / 3600:.2f} 小時，直到下一次排定的運行時間: {next_run_time.isoformat()}。")
        
        # --- 優雅關閉的關鍵實現 ---
        # 使用 asyncio.wait_for 來等待 shutdown_event 被設置，但最多只等待 sleep_seconds。
        # 這種寫法比手動迴圈 sleep 更簡潔且符合 asyncio 的慣例。
        # 如果在休眠期間收到關閉信號，`wait_for` 會立即結束，使迴圈能夠快速響應並退出。
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            # 等待時間到，這是正常情況，繼續下一次迴圈。
            pass

if __name__ == "__main__":
    # 為 runner 腳本設定日誌記錄
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 註冊信號處理器以實現優雅關閉
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    asyncio.run(main_loop())
    logging.info("工人迴圈已優雅地關閉。")