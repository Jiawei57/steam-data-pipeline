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
 
# --- 組態與設定 ---
 
# 設定日誌記錄。預設為 INFO 等級，以獲得更簡潔的生產環境日誌。
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
# 將過於詳細的 httpx 日誌記錄器設為靜音，以防止它在 URL 中洩漏 API 金鑰。
logging.getLogger("httpx").setLevel(logging.WARNING)
 
load_dotenv() # 在本地開發時，從 .env 檔案載入環境變數
 
# 從環境變數載入憑證
DATABASE_URL = os.getenv("DATABASE_URL")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")
USE_PREMIUM_PROXY = os.getenv("USE_PREMIUM_PROXY", "false").lower() == "true" 
 
if not DATABASE_URL:
    logging.error("未設定 DATABASE_URL 環境變數。應用程式無法啟動。")
    raise ValueError("未設定 DATABASE_URL 環境變數。")
 
# --- 全域快取與客戶端 ---
 
# 一個用於儲存 Twitch 權杖的簡單記憶體內快取
twitch_token_cache: Dict[str, Any] = {"token": None, "expires_at": datetime.utcnow()}
 
# --- 常數設定 ---
# 降低批次大小以減少在資源受限平台（如 Render 免費方案）上的記憶體使用量。
# 較小的批次會創建較少的並發 asyncio 任務，從而防止記憶體尖峰。
BATCH_SIZE = int(os.getenv("SCRAPER_BATCH_SIZE", 20)) 
# 將預設並發數從 10 降至 1，以降低請求頻率，避免 429 速率限制錯誤。
# 這是一個更「有禮貌」且更可靠的設定。
CONCURRENCY_LIMIT = int(os.getenv("SCRAPER_CONCURRENCY_LIMIT", 1)) 
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
RETRIABLE_STATUSES = {403, 407, 429, 500, 502, 503, 504} # 定義哪些 HTTP 狀態碼是可重試的
 
# --- 擬人化策略：User-Agent 輪換 ---
# 一個常見的 User-Agent 列表，用於在每次請求時輪換。
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
] 
 
http_client = httpx.AsyncClient(
    timeout=30.0, 
    follow_redirects=True, 
    # 這裡設定的預設 User-Agent 會在每次請求時被覆蓋。
    headers={"User-Agent": random.choice(USER_AGENTS)} 
) 
 
# --- 資料庫設定 (SQLAlchemy ORM) ---
 
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
    tags = Column(String) # 此欄位目前未被填充，可以移除或供日後使用
    metadata_last_updated = Column(TIMESTAMP(timezone=True), server_default=text('CURRENT_TIMESTAMP'), onupdate=lambda: datetime.now(timezone.utc)) 
 
class GamesTimeseries(Base):
    __tablename__ = "games_timeseries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    app_id = Column(String) # 下方的複合索引已包含此欄位
    timestamp = Column(TIMESTAMP(timezone=True), default=lambda: datetime.now(timezone.utc), index=True) 
    price_numeric = Column(Numeric(10, 2)) # 例如：19.99
    price_currency = Column(String(3)) # 例如：'USD', 'EUR'
    discount_percent = Column(Integer)
    player_count = Column(Integer)
    streamer_count = Column(Integer) 
 
    __table_args__ = (
        # 建立複合索引，以高效查詢特定遊戲的歷史數據
        Index('ix_games_timeseries_app_id_timestamp', 'app_id', 'timestamp'), 
    ) 
 
class ScrapingState(Base):
    """一個簡單的鍵值對資料表，用於儲存爬取過程的狀態（實現分布式鎖）。"""
    __tablename__ = "scraping_state"
    key = Column(String, primary_key=True)
    value = Column(String) 
 
# --- 外部 API 服務 ---
 
async def make_request_with_retry(method: str, url: str, use_proxy: bool = False, **kwargs) -> Optional[httpx.Response]:
    """執行一個帶有重試邏輯的 HTTP 請求，並使用信號量來限制並發數。"""
    max_retries = 3
    base_delay = 10.0 # 增加基礎延遲時間，以更「有禮貌」地對待 API 伺服器
 
    request_url = url
    # 如果需要代理且金鑰存在，則建構 ScraperAPI 的請求 URL
    if use_proxy and SCRAPERAPI_KEY:
        payload = {'api_key': SCRAPERAPI_KEY, 'url': url, 'keep_headers': 'true'}
        # --- 面向未來的設計：為高級/住宅代理新增一個開關 ---
        # 如果您未來升級代理方案以獲得更高的成功率，
        # 只需將 USE_PREMIUM_PROXY 環境變數設為 "true"。
        if USE_PREMIUM_PROXY:
            payload['premium'] = 'true' # 此參數通常用於在代理服務中啟用住宅 IP
        request_url = 'http://api.scraperapi.com/?' + urlencode(payload)
        logging.debug(f"正在為 URL 使用代理: {url}")
    elif use_proxy and not SCRAPERAPI_KEY:
        logging.error(f"嘗試為 {url} 使用代理，但未設定 SCRAPERAPI_KEY。將直接發出請求。")
 
    for attempt in range(max_retries):
        try:
            async with semaphore:
                # --- 擬人化策略：在每次請求前加入隨機延遲 ("Jitter") ---
                # 這能平滑請求速率，使其看起來不那麼像機器人。
                jitter_delay = 4 + random.uniform(1, 3) # 等待 5 到 7 秒
                await asyncio.sleep(jitter_delay)
 
                # --- 擬人化策略：為每次請求輪換 User-Agent ---
                request_headers = kwargs.pop('headers', {}).copy()
                request_headers['User-Agent'] = random.choice(USER_AGENTS)
                if method.upper() == 'GET':
                    response = await http_client.get(request_url, headers=request_headers, **kwargs)
                elif method.upper() == 'POST':
                    response = await http_client.post(request_url, headers=request_headers, **kwargs)
                else:
                    logging.error(f"不支援的 HTTP 方法: {method}")
                    return None
                response.raise_for_status()
                return response # 成功，直接返回

        except httpx.HTTPStatusError as exc:
            # 捕捉 HTTP 狀態碼錯誤
            e = exc  # 將異常儲存起來以供後續使用
            if exc.response.status_code == 404:
                logging.info(f"請求 {method} {url} 返回 404 Not Found。將此視為非錯誤性的空回應。")
                return None  # 404 是最終狀態，直接返回 None，不再重試
            is_retriable_status = exc.response.status_code in RETRIABLE_STATUSES
            is_network_error = False

        except (httpx.RequestError, httpx.ProxyError, json.JSONDecodeError) as exc:
            # 捕捉網絡層或解析錯誤
            e = exc  # 將異常儲存起來以供後續使用
            is_retriable_status = False
            is_network_error = True
 
        # --- 重試邏輯現在位於 try-except 區塊之外，以正確處理所有可重試的錯誤類型 ---
        if is_retriable_status or is_network_error:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                logging.warning(f"請求 {method} {url} 因 {type(e).__name__} 失敗。將在 {delay:.2f} 秒後重試... (第 {attempt + 1}/{max_retries} 次嘗試)")
                await asyncio.sleep(delay)
            else:
                logging.error(f"請求 {method} {url} 在 {max_retries} 次嘗試後失敗。放棄。最終錯誤: {e}")
        else:  # 處理不可重試的客戶端錯誤（例如 400, 401）
            logging.error(f"請求 {method} {url} 遇到不可恢復的客戶端錯誤。不再重試。錯誤: {e}")
            break  # 對於不可重試的錯誤，退出迴圈

    return None
 
async def get_twitch_token() -> Optional[str]:
    """獲取 Twitch 應用程式存取權杖，並使用快取來避免重複請求。"""
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        logging.warning("未設定 Twitch 憑證。將跳過抓取實況主數量。")
        return None
 
    # 如果快取的權杖仍然有效，則直接返回
    if twitch_token_cache["token"] and twitch_token_cache["expires_at"] > datetime.now(timezone.utc):
        return twitch_token_cache["token"]
 
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials",
    } 
    # 此請求不需要代理
    response = await make_request_with_retry('POST', url, params=params)
    if not response:
        raise Exception("獲取 Twitch 權杖的請求在重試後仍返回 None。")
    data = response.json()
    token = data["access_token"]
    expires_in = data.get("expires_in", 3600) # 預設為 1 小時
    
    # 快取新的權杖及其過期時間
    twitch_token_cache["token"] = token
    twitch_token_cache["expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=expires_in * 0.9) # 在過期前刷新
    
    logging.info("成功獲取並快取了新的 Twitch 權杖。")
    return token
 
async def fetch_paginated_list(base_url: str, limit: int, selector: str, id_extractor) -> List[str]:
    """一個通用的函數，用於抓取 Steam 上的分頁列表。"""
    all_app_ids = []
    page = 1
    while len(all_app_ids) < limit:
        # 在每次迴圈開始時檢查關閉信號
        if shutdown_event.is_set():
            logging.warning("在分頁抓取過程中收到關閉信號。正在中止列表檢索。")
            break
 
        # Steam 搜尋頁面使用 'page' 查詢參數
        url = f"{base_url}&page={page}"
        logging.info(f"正在抓取第 {page} 頁: {url}")
        # 此請求訪問 store.steampowered.com，因此我們使用代理。
        response = await make_request_with_retry('GET', url, use_proxy=True)
        if not response:
            logging.error(f"在所有重試後仍無法抓取第 {page} 頁。正在中止分頁抓取。")
            break # 如果某一頁完全失敗，則停止抓取更多頁面
        soup = BeautifulSoup(response.text, "html.parser")
        page_app_ids = [id_extractor(row) for row in soup.select(selector) if id_extractor(row)]
        if not page_app_ids:
            break  # 如果某一頁沒有結果，則停止。這不是一個錯誤。
        all_app_ids.extend(page_app_ids)
        page += 1
        # --- 擬人化策略：在抓取頁面之間加入隨機延遲 ---
        page_delay = 2 + random.uniform(0.5, 1.5) # 等待 2.5 到 3.5 秒
        await asyncio.sleep(page_delay)
    return all_app_ids[:limit]
 
async def fetch_all_app_ids() -> List[str]:
    """從官方 Steam API 獲取所有 App ID。"""
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/" # 這是一個公開 API，不需要代理。
    try:
        response = await make_request_with_retry('GET', url)
        if not response:
            raise Exception("請求返回 None")
        data = response.json()
        # API 返回一個 {"appid": 123, "name": "遊戲名稱"} 的列表
        apps = data.get("applist", {}).get("apps", [])
        app_ids = [str(app.get("appid")) for app in apps if app.get("appid")]
        logging.info(f"成功從 Steam 獲取了 {len(app_ids)} 個總 App ID。")
        return app_ids
    except Exception as e:
        logging.error(f"獲取所有 App ID 失敗: {e}")
        return []
 
async def fetch_top_selling_ids(limit: int = 500) -> List[str]:
    """從 Steam 搜尋中獲取暢銷遊戲的 ID。"""
    logging.info("正在抓取暢銷榜列表...")
    ids = await fetch_paginated_list(
        base_url="https://store.steampowered.com/search/?filter=topsellers",
        limit=limit,
        selector="a.search_result_row",
        id_extractor=lambda row: row.get("data-ds-appid")
    ) 
    if not ids:
        raise Exception("在多次重試後仍無法獲取暢銷遊戲 ID。")
    return ids
 
async def fetch_most_played_ids() -> List[str]:
    """從 Steam 排行榜中獲取最多人玩的遊戲 ID。"""
    logging.info("正在抓取最多人玩列表...")
    url = "https://store.steampowered.com/charts/mostplayed"
    # 此請求訪問 store.steampowered.com，因此我們使用代理。
    response = await make_request_with_retry('GET', url, use_proxy=True)
    if not response:
        raise Exception("在多次重試後仍無法獲取最多人玩的頁面。")
    soup = BeautifulSoup(response.text, "html.parser")
    ids = [row.get("data-appid") for row in soup.select("tr.weeklytopsellers_TableRow_2-RN6") if row.get("data-appid")]
    if not ids:
        raise Exception("在多次重試後仍無法獲取最多人玩的 ID。")
    return ids
 
async def fetch_game_details(app_id: str) -> Optional[Dict[str, Any]]:
    """從 Steam API 獲取單個遊戲的詳細元數據。"""
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}" # 這是內部商店 API，需要代理。
    try:
        # 此請求訪問 store.steampowered.com，因此我們使用代理。
        response = await make_request_with_retry('GET', url, use_proxy=True)
        if not response:
            raise Exception("請求返回 None")
        data = response.json().get(app_id, {})
        if data.get("success"):
            details = data.get("data", {})
            return {
                "app_id": app_id,
                "name": details.get("name", "").strip(), # 使用 strip() 清理名稱中的多餘空格
                "type": details.get("type"),
                "release_date": details.get("release_date", {}).get("date"),
                "developer": ", ".join(details.get("developers", [])),
                "publisher": ", ".join(details.get("publishers", [])),
                "genres": ", ".join([g["description"] for g in details.get("genres", [])]),
                # 在這裡也提取價格資訊，以避免第二次 API 呼叫。
                # price_overview 物件包含原始的數字值，這比解析字串要好得多。
                "price_overview": details.get("price_overview", {
                    "final_formatted": "N/A",
                    "discount_percent": 0
                })
            } 
    except Exception as e:
        logging.error(f"獲取 app_id {app_id} 的詳細資訊失敗: {e}")
    return None
 
def normalize_game_name(name: str) -> str:
    """移除常見的、會干擾 API 查詢的符號。"""
    return name.replace('™', '').replace('®', '').strip()
 
async def fetch_timeseries_data(app_id: str, game_name: str, price_info: Optional[Dict], twitch_token: Optional[str]) -> Optional[Dict[str, Any]]:
    """獲取單個遊戲的動態時間序列數據。"""
    # 1. 獲取玩家數量
    player_count = 0
    if STEAM_API_KEY:
        try:
            player_url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}&key={STEAM_API_KEY}" 
            # 移除 use_proxy=True。此 API 通常不需要代理，直接請求更可靠且不消耗代理額度。
            player_response = await make_request_with_retry('GET', player_url)
            if player_response:
                player_data = player_response.json().get("response", {})
                player_count = player_data.get("player_count", 0)
            # 如果 player_response 為 None (例如 404)，則 player_count 保持為 0，不視為錯誤
        except Exception as e: # 捕捉 JSON 解碼等其他潛在錯誤
            logging.warning(f"無法獲取 app_id {app_id} 的玩家數量。")
 
    # 2. 獲取實況主數量
    streamer_count = 0
    if twitch_token and game_name:
        try:
            normalized_name = normalize_game_name(game_name)
            stream_url = "https://api.twitch.tv/helix/streams"
            headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {twitch_token}"}
            # 數據準確性提升：每次請求最多獲取 100 個直播（API 允許的最大值），
            # 以獲得比預設 20 個更準確的實況主數量。
            params = {"game_name": normalized_name, "first": 100}
 
            stream_response = await make_request_with_retry('GET', stream_url, headers=headers, params=params)
            if not stream_response:
                raise Exception("獲取實況主數量的請求返回 None")
            streamer_count = len(stream_response.json().get("data", []))
        except Exception:
            logging.warning(f"無法獲取遊戲 '{game_name}' 的實況主數量。")
 
    # 3. 使用預先獲取的價格資訊
    price_numeric = None
    price_currency = None
    discount = 0
    if price_info:
        # 'initial' 是以最小貨幣單位表示的價格（例如：美分）
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
 
# --- 主管道邏輯 ---
 
async def scrape_and_store_data():
    """主管道函數，負責協調整個爬取過程，並支援優雅關閉。"""
    db = SessionLocal()
    try:
        # 確保資料表存在
        Base.metadata.create_all(bind=engine)
 
        # --- 使用資料庫實現的鎖，以防止並發運行 ---
        # 檢查是否有爬取任務正在進行中
        is_running_record = db.query(ScrapingState).filter(ScrapingState.key == 'is_scraping_active').first()
        if is_running_record and is_running_record.value == 'true':
            # 檢查鎖是否已過期（例如，來自上一次的崩潰）
            last_started_record = db.query(ScrapingState).filter(ScrapingState.key == 'last_started_utc').first()
            if last_started_record:
                last_started_time = datetime.fromisoformat(last_started_record.value)
                if datetime.now(timezone.utc) - last_started_time > timedelta(hours=2):
                    logging.warning("發現一個超過 2 小時的過期鎖。正在覆蓋並開始新的爬取。")
                else:
                    logging.warning("根據資料庫鎖，爬取過程已在運行中。正在跳過新的觸發。")
                    return
 
        # 設定鎖
        db.merge(ScrapingState(key='is_scraping_active', value='true'))
        db.merge(ScrapingState(key='last_started_utc', value=datetime.now(timezone.utc).isoformat()))
        db.commit()
        logging.info("已獲取資料庫鎖。正在開始數據爬取過程...")
        # --- 鎖邏輯結束 ---
 
        # --- 智慧混合策略：抓取一個高價值的遊戲池 ---
        top_selling_ids, most_played_ids = [], []
        try:
            logging.info("正在從暢銷榜和最多人玩列表中抓取候選遊戲...")
            
            top_selling_ids_task = fetch_top_selling_ids(limit=500)
            most_played_ids_task = fetch_most_played_ids()
 
            results = await asyncio.gather(top_selling_ids_task, most_played_ids_task, return_exceptions=True)
            top_selling_ids = results[0] if not isinstance(results[0], Exception) else []
            most_played_ids = results[1] if not isinstance(results[1], Exception) else []
        except Exception as e:
            logging.critical(f"在收集初始遊戲列表時發生意外錯誤: {e}", exc_info=True)
 
        # 合併並去重列表，以創建一個高價值的遊戲池
        candidate_app_ids = sorted(list(set(top_selling_ids + most_played_ids)))
 
        if not candidate_app_ids:
            logging.warning("正在中止爬取：無法獲取任何候選 App ID。")
            return
        
        twitch_token = await get_twitch_token()
 
        total_apps = len(candidate_app_ids)
        logging.info(f"正在以 {BATCH_SIZE} 為批次大小，處理一個包含 {total_apps} 個遊戲的高價值池。")
 
        for i in range(0, total_apps, BATCH_SIZE):
            batch_ids = candidate_app_ids[i:i + BATCH_SIZE]
            
            # 優雅關閉檢查
            if shutdown_event.is_set():
                logging.info("收到關閉信號。正在停止處理下一個批次。")
                break
 
            logging.info(f"正在處理批次 {i//BATCH_SIZE + 1}/{(total_apps + BATCH_SIZE - 1)//BATCH_SIZE} (應用程式 {i+1}-{i+len(batch_ids)})...")
 
            # 1. 抓取並更新/插入該批次的元數據
            metadata_tasks = [fetch_game_details(app_id) for app_id in batch_ids]
            metadata_results = await asyncio.gather(*metadata_tasks)
            valid_metadata = [m for m in metadata_results if m and m.get("name")]
 
            if valid_metadata:
                # 為資料庫準備一個乾淨的字典列表，排除臨時的 'price_overview' 鍵。
                metadata_to_upsert = [{k: v for k, v in m.items() if k != 'price_overview'} for m in valid_metadata]
                
                stmt = pg_insert(GamesMetadata).values(metadata_to_upsert)
                update_stmt = stmt.on_conflict_do_update(
                    index_elements=['app_id'],
                    set_={col.name: getattr(stmt.excluded, col.name) for col in GamesMetadata.__table__.columns if col.name != 'app_id'}
                ) 
                db.execute(update_stmt)
                db.commit()
                logging.info(f"已為此批次更新/插入了 {len(valid_metadata)} 筆元數據記錄。")
 
            # 2. 抓取並插入該批次的時間序列數據
            # 穩健性提升：為任何具有名稱的有效元數據條目抓取時間序列數據。
            # 即使類型不嚴格為 'game'，玩家數量 API 也可能返回數據。
            # 這可以防止在元數據抓取不穩定時，丟失像 CS2 這樣熱門遊戲的關鍵數據。
            apps_to_fetch = [(m["app_id"], m["name"], m.get("price_overview", {})) for m in valid_metadata if m.get("name")]
            if apps_to_fetch:
                # 如果我們將要處理一個未標記為 'game' 類型的應用，則記錄日誌
                non_game_apps = [m for m in valid_metadata if m.get("type") != "game"]
                if non_game_apps:
                    logging.warning(f"發現 {len(non_game_apps)} 個非 'game' 類型的應用，但仍會嘗試抓取時間序列數據。範例 app_id: {non_game_apps[0]['app_id']}")
                timeseries_tasks = [fetch_timeseries_data(app_id, name, price_info, twitch_token) for app_id, name, price_info in apps_to_fetch]
                timeseries_results = await asyncio.gather(*timeseries_tasks)
                valid_timeseries = [t for t in timeseries_results if t]
 
                if valid_timeseries:
                    db.bulk_insert_mappings(GamesTimeseries, valid_timeseries)
                    db.commit()
                    logging.info(f"已為此批次插入了 {len(valid_timeseries)} 筆時間序列記錄。")
            
            # --- 擬人化策略：在批次之間加入一個隨機的、較長的暫停 ---
            batch_pause = 60 + random.uniform(15, 45) # 暫停 75 到 105 秒
            logging.info(f"為尊重 API 速率限制，在處理下一個批次前將暫停 {batch_pause:.2f} 秒...")
            await asyncio.sleep(batch_pause)
 
    except Exception as e:
        logging.error(f"在爬取管道中發生錯誤: {e}", exc_info=True)
        db.rollback()
    finally:
        # 在資料庫中釋放鎖
        db.merge(ScrapingState(key='is_scraping_active', value='false'))
        db.commit()
        db.close()
        if shutdown_event.is_set():
            logging.info("爬取過程已優雅地關閉。")
        else:
            logging.info("爬取過程已完成。")
 
# --- FastAPI 應用程式 ---
 
app = FastAPI(
    title="Steam 數據管道",
    description="一個自動化管道，用於抓取所有 Steam 遊戲數據並將其儲存在 PostgreSQL 資料庫中。",
    version="3.0.0", # 因重大架構改進而提升版本號
) 
 
# --- 資料庫會話的依賴項 ---
def get_db():
    """為每個請求獲取一個新的資料庫會話的依賴項。"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
 
@app.get("/games", summary="獲取一些範例遊戲記錄", tags=["數據"])
def get_games(db: Session = Depends(get_db)):
    """
    一個簡單的端點，用於從資料庫中檢索前 5 筆遊戲元數據記錄，
    以驗證數據是否存在。
    """
    games = db.query(GamesMetadata).limit(5).all()
    if not games:
        return {"message": "未找到遊戲數據。爬蟲是否已運行？"}
    return games
 
@app.get("/", summary="健康檢查", tags=["狀態"])
async def root():
    """提供一個簡單的健康檢查端點。"""
    return {"status": "ok", "message": "Steam 爬蟲服務正在運行。"}
 
@app.get("/trigger-scrape", summary="觸發數據爬取", tags=["操作"])
async def trigger_scrape(background_tasks: BackgroundTasks):
    """
    將數據爬取和儲存過程作為背景任務觸發。
    此端點由 Render Cron Job 呼叫。
    """
    background_tasks.add_task(scrape_and_store_data)
    return {"message": "數據爬取過程已在背景中觸發。"}
 
# --- 全域關閉事件與處理器 ---
shutdown_event = asyncio.Event()
 
@app.on_event("shutdown")
async def on_shutdown():
    """設定關閉事件並優雅地關閉 httpx 客戶端。"""
    logging.info("收到關閉信號。正在準備關閉資源。")
    shutdown_event.set() # 向爬蟲發出停止信號
    logging.info("正在關閉 HTTP 客戶端...")
    await http_client.aclose()
