# Steam 數據管道

一個穩健、自動化的數據管道，用於從 Steam 抓取遊戲數據並將其儲存到 PostgreSQL 資料庫中。此專案包含一個用於抓取數據的背景工人 (Background Worker) 和一個用於基本互動的 FastAPI 介面。

## 功能特性

- **混合抓取策略**: 從 Steam 的「暢銷商品」和「最多人玩」排行榜中獲取高價值的遊戲池。
- **高韌性與穩健性**: 針對網路和 API 錯誤，實現了帶有指數退避策略的自動重試機制。
- **代理輪換**: 利用住宅代理池來避免 IP 封鎖，確保高成功率。
- **選擇性代理**: 智能地僅對敏感網域（如 `store.steampowered.com`）的請求應用代理，同時對公開 API（如 `api.steampowered.com` 和 Twitch）使用直接連線。
- **並發處理**: 使用 `asyncio` 和信號量 (Semaphore) 以並發批次的方式處理數據，最大化效率。
- **優雅關閉**: 背景工人可以被優雅地關閉，確保正在進行中的任務在退出前能順利完成。
- **高效資料庫操作**: 使用 SQLAlchemy ORM，結合批次插入和 "upsert" (on-conflict-do-update) 邏輯，實現高效能的數據儲存。
- **數據整合**: 使用 Twitch API 的直播主數量來豐富 Steam 數據。

## 系統架構

- **資料庫**: 一個託管在 Render 上的 PostgreSQL 資料庫，用於儲存所有抓取到的數據（`games_metadata` 和 `games_timeseries` 表）。
- **背景工人 (Background Worker)**: 一個作為 Render 背景工人運行的 Python 應用程式。它透過 `runner.py` 執行主要的抓取邏輯 (`scrape_and_store_data`)。
- **API (可選)**: 程式碼中包含一個輕量級的 FastAPI 應用，提供健康檢查和手動觸發端點。在背景工人的設定中，此 API 不會對外公開。

## 本地開發環境設定

1.  **克隆儲存庫**:
    ```bash
    git clone <您的儲存庫 URL>
    cd <專案資料夾名稱>
    ```

2.  **建立虛擬環境**:
    ```bash
    python -m venv venv
    source venv/bin/activate  # 在 Windows 上，使用 `venv\Scripts\activate`
    ```

3.  **安裝依賴套件**:
    您的專案中應已包含 `requirements.txt` 檔案。執行以下指令進行安裝：
    ```bash
    pip install -r requirements.txt
    ```

4.  **設定環境變數**:
    在專案根目錄下建立一個 `.env` 檔案，並填入您的憑證。您可以使用本地的 PostgreSQL 實例，或使用您在 Render DB 上的「外部連線字串」。
    ```
    # .env 檔案內容範例
    DATABASE_URL=postgresql://user:password@host:port/database
    STEAM_API_KEY=YOUR_STEAM_API_KEY
    TWITCH_CLIENT_ID=YOUR_TWITCH_CLIENT_ID
    TWITCH_CLIENT_SECRET=YOUR_TWITCH_CLIENT_SECRET
    PROXY_URLS=http://user:pass@host1:port1,http://user:pass@host2:port2 # 住宅代理 URL，以逗號分隔
    ```

5.  **在本地運行應用程式**:
    您可以運行 FastAPI 伺服器來測試 API 端點：
    ```bash
    uvicorn main:app --reload
    ```
    或者，直接運行爬蟲腳本，模擬它在 Render 上的運行方式：
    ```bash
    python runner.py
    ```

## 在 Render 上部署

此專案被設計為在 Render 上作為 **背景工人 (Background Worker)** 部署。

1.  **建立 PostgreSQL 資料庫**:
    - 在 Render 中，建立一個新的 **PostgreSQL** 服務。
    - 使用 `Free` 方案並選擇一個區域（例如 `Oregon (US West)`）。

2.  **建立背景工人**:
    - 建立一個新的 **Background Worker** 並將其連接到您的 GitHub 儲存庫。
    - **啟動指令 (Start Command)**: `python runner.py`
    - 確保區域與您的資料庫相同。

3.  **設定環境變數**:
    - 在背景工人的 **Environment** 頁籤中，透過連結到您的 PostgreSQL 服務的內部連線字串來新增 `DATABASE_URL`。
    - 新增您的 `STEAM_API_KEY`, `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`, 和 `PROXY_URLS` (以逗號分隔)。

4.  **自動部署**:
    每當您 `git push` 到 `main` 分支時，Render 都會自動部署新的變更。

若需更詳細的逐步指南，請參考 `RENDER_DEPLOYMENT_GUIDE.txt` 檔案。

## 如何驗證數據

使用像 DBeaver 或 pgAdmin 這樣的客戶端工具連接到您的 PostgreSQL 資料庫，並執行 SQL 查詢來檢查 `games_metadata` 和 `games_timeseries` 表。請參考 `RENDER_DEPLOYMENT_GUIDE.txt` 中的「驗證」部分以獲取查詢範例。