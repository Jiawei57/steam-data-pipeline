# Steam 遊戲趨勢分析管道 (Render 版本)

這是一個部署在 **Render** 上的全自動數據管道，旨在從 Steam 平台大規模地擷取**所有遊戲**的元數據與趨勢數據，並將其結構化地存儲在 **PostgreSQL** 資料庫中，以供後續的商業智慧 (BI) 分析與視覺化應用。

此專案採用了現代化的 PaaS (平台即服務) 架構，透過「基礎設施即代碼」實現了極簡的部署與維護流程。

> **重要提示**: 由於目標是抓取 Steam 全部的數十萬款遊戲，單次完整任務的執行時間可能長達數小時。在 Render 的免費方案上執行此任務可能會因資源限制而中斷。建議將此作為一次性的數據初始化任務，或在更強大的付費方案 (如 Render Background Worker) 上運行。

## 專案亮點 (Key Features)

*   **高效能非同步處理 (High-Performance Async IO)**：採用 `FastAPI` 搭配 `httpx` 與 `asyncio`，能夠並行處理對多個 Steam API 的大量請求，極大地壓縮了數據獲取時間。
*   **穩健的批次處理 (Robust Batch Processing)**：以批次方式處理數十萬筆資料，有效控制記憶體使用並規避 API 速率限制。
*   **增量更新 (Upsert Logic)**：利用 PostgreSQL 的 `ON CONFLICT DO UPDATE` 功能，高效地插入新遊戲資料或更新現有遊戲資料，避免重複工作。
*   **安全的密鑰管理 (Secure Secret Management)**：透過 Render 平台的環境變數管理 API 金鑰，符合雲端安全最佳實踐。
*   **聲明式部署 (Declarative Deployment)**：僅需一個 `render.yaml` 檔案即可定義所有雲端資源（資料庫、Web 服務、排程），實現「基礎設施即代碼」。
*   **低成本無伺服器架構 (Cost-Effective Serverless)**：基於 Render 的免費方案，Web 服務在閒置時會自動休眠，待機成本為零。

## 技術架構 (Tech Stack)

*   **雲端平台**: Render
*   **Web 服務**: FastAPI on Python 3.11
*   **資料儲存**: PostgreSQL
*   **排程**: Render Cron Jobs
*   **核心技術**:
    *   **資料庫 ORM**: SQLAlchemy
    *   **Web Scraping**: BeautifulSoup4
    *   **API 請求**: `httpx` (非同步)
    *   **資料處理**: Pandas
*   **視覺化工具 (建議)**: Looker Studio

## 部署指南 (Deployment Guide)

本專案的部署流程分為兩個部分：建立資料庫和建立背景工人服務。

### 前置準備
1.  一個 GitHub 帳號。
2.  一個 Render 帳號 (可使用 GitHub 帳號登入)。
3.  將此專案 Fork 到您自己的 GitHub 帳號下，或是建立一個新的 repository 並將程式碼推送上去。

### 步驟一：建立 PostgreSQL 資料庫
1.  在 Render 儀表板，點擊 **New +** > **PostgreSQL**。
2.  設定一個名稱 (例如 `steam-db`) 和資料庫名稱 (例如 `steam_data`)。
3.  選擇 `Free` 方案並點擊 **Create Database**。

### 步驟二：建立 Background Worker
1.  在 Render 儀表板，點擊 **New +** > **Background Worker**。
2.  連接您存放此專案的 GitHub repository。
3.  設定一個名稱 (例如 `steam-scraper-worker`)。
4.  **Start Command** 填入: `python runner.py`
5.  選擇 `Free` 方案並點擊 **Create Background Worker**。
6.  部署開始後，前往服務的 **Environment** 頁籤，手動新增以下環境變數並儲存：
    *   `DATABASE_URL`: 點擊輸入框，從下拉選單中選擇您剛剛建立的 `steam-db` 資料庫的 `Internal Connection String`。
    *   `STEAM_API_KEY`: 您的 Steam API 金鑰。
    *   `TWITCH_CLIENT_ID`: (可選) 您的 Twitch Client ID。
    *   `TWITCH_CLIENT_SECRET`: (可選) 您的 Twitch Client Secret。
7.  儲存環境變數後，Render 會自動重新部署您的服務。

部署完成！您的背景工人將在啟動後自動開始執行數據抓取任務。如果它被重啟，它會自動從上次的進度繼續。

### 更新服務
每當您將新的程式碼改動推送到 GitHub repository 的主分支時，Render 都會**自動**抓取最新的程式碼並重新部署您的背景工人服務，實現真正的持續部署 (Continuous Deployment)。

## 本地開發 (Local Development)

1.  在本地安裝一個 PostgreSQL 資料庫 (推薦使用 Docker)。
2.  建立一個 `.env` 檔案，並填入您的本地資料庫 URL 和 API 金鑰 (參考 `.env.example`)。
3.  安裝 Python 依賴：`pip install -r requirements.txt`。
4.  啟動本地伺服器：`uvicorn main:app --reload`。
5.  若要執行爬蟲，請運行：`python runner.py`。

## 連接視覺化工具
1.  在 Render 儀表板中，找到您的 PostgreSQL 資料庫服務。
2.  在 **Info** 頁籤下，找到 **External Connection String** (外部連線字串)。
3.  將此連線字串的資訊（主機、使用者、密碼、資料庫名稱）填入 Looker Studio 的 PostgreSQL 連接器中。
4.  您現在可以開始基於 `games_metadata` 和 `games_timeseries` 這兩個資料表來建立您的儀表板了。

---

*Render 的免費方案非常適合個人專案。如果您的專案需要更高的效能或 24/7 運行，可以隨時升級到付費方案。*
