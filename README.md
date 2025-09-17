# Steam 市場洞察數據管道 (Steam Market Insight Data Pipeline)

## 專案目標 (What is this?)

本專案是一個全自動化的數據管道，旨在每日從 Steam 和 Twitch 平台採集遊戲市場數據。它會抓取如**玩家數量、遊戲價格、折扣資訊、實況主流行度**等關鍵指標，並將這些數據整合、清洗後存入一個 PostgreSQL 資料庫。

整個系統被設計為在雲端自動運行，為後續的市場分析、趨勢追蹤和數據視覺化應用提供一個乾淨、可靠且持續更新的數據源。

---

## 核心功能亮點 (Key Features)

*   **端到端自動化 (End-to-End Automation)**: 每日自動執行，從數據採集、清洗、整合到儲存，無需人工干預。
*   **為穩定性而生 (Built for Reliability)**:
    *   **智慧重試機制**: 當遇到網路不穩或 API 暫時性錯誤時，系統會自動以指數級增加的延遲進行重試，大大提高了數據採集的成功率。
    *   **優雅關閉 (Graceful Shutdown)**: 在服務重啟或部署更新時，系統能安全地完成當前的批次任務再關閉，有效防止數據損壞或遺失。
    *   **並發安全鎖**: 透過資料庫實現了分布式鎖，確保即使在多實例的環境下，也只有一個爬蟲在工作，避免了數據重複和衝突。
*   **擬人化反爬蟲策略 (Human-like Scraping)**:
    *   **代理整合**: 透過 ScraperAPI 整合代理服務，並預留了升級至住宅 IP 的開關。
    *   **擬人化模擬**: 透過輪換 User-Agent 和在請求間加入隨機延遲，模擬真實用戶行為，大幅提升抓取成功率。
*   **雲端原生架構 (Cloud-Native Architecture)**:
    *   **容器化思維**: 專為在 Render 等 PaaS 平台上作為背景工人 (Background Worker) 部署而設計。
    *   **環境變數驅動**: 所有敏感金鑰與配置均透過環境變數管理，符合十二因子應用 (Twelve-Factor App) 規範。
*   **高效能數據處理 (Efficient & Fast)**:
    *   **異步 I/O**: 全面採用 `asyncio` 和 `httpx` 進行高並發的網路請求。
    *   **批次處理**: 數據以批次方式處理，並透過 SQLAlchemy Core 的 `pg_insert` 實現高效的 "Upsert" 操作，最大化資料庫寫入效能。

## 系統架構 (System Architecture)

本專案由兩個在 Render 平台上運行的核心服務組成，其數據流如下：

```mermaid
graph TD
    subgraph Render Cloud
        A[Cron Job (每日定時觸發)] --> B{背景工人 (Python 爬蟲)};
        B -- 抓取數據 --> C[Steam & Twitch APIs];
        B -- 儲存數據 --> D[(PostgreSQL 資料庫)];
    end

    subgraph External Services
        C
    end

    style B fill:#f9f,stroke:#333,stroke-width:2px
    style D fill:#ccf,stroke:#333,stroke-width:2px
```

1.  **PostgreSQL 資料庫 (`steam-db`)**:
    *   **平台**: Render
    *   **作用**: 作為數據的持久化儲存層。包含 `games_metadata`（靜態元數據）、`games_timeseries`（動態時間序列數據）和 `scraping_state`（用於實現分布式鎖）三個核心資料表。

2.  **背景工人 (`steam-scraper-worker`)**:
    *   **平台**: Render
    *   **作用**: 核心的數據處理服務。它是一個無狀態的 Python 應用，由 `runner.py` 啟動，每日在隨機的排定時間觸發 `main.py` 中的 `scrape_and_store_data` 函數，執行完整的數據管道任務。

## 技術棧 (Technology Stack)

*   **語言**: Python 3.11
*   **核心框架**: `asyncio`
*   **網路請求**: `httpx`
*   **網頁解析**: `BeautifulSoup4`
*   **資料庫 ORM**: `SQLAlchemy`
*   **雲端平台**: Render
*   **資料庫**: PostgreSQL
*   **代理服務**: ScraperAPI

## 如何開始 (Getting Started)

### 1. 雲端部署 (推薦)

要將此數據管道部署到您自己的 Render 帳號，請遵循以下指南中的詳細步驟：

> **RENDER_DEPLOYMENT_GUIDE.txt**

### 2. 本地開發

如果您想在本地機器上進行開發或測試，請按照以下步驟操作：

1.  **環境準備**:
    *   確保您已安裝 Python 3.11。
    *   確保本地或遠端有一個可用的 PostgreSQL 資料庫。

2.  **克隆專案**:
    ```bash
    git clone https://github.com/your-username/your-repo-name.git
    cd your-repo-name
    ```

3.  **設定虛擬環境**:
    ```bash
    # 建立虛擬環境
    python -m venv venv
    # 啟用虛擬環境 (macOS/Linux)
    source venv/bin/activate
    # 或者 (Windows)
    # venv\Scripts\activate
    ```

4.  **安裝依賴**:
    ```bash
    pip install -r requirements.txt
    ```

5.  **配置環境變數**:
    在專案根目錄下建立一個名為 `.env` 的檔案，並填入必要的金鑰與設定。您可以複製 `.env.example` (如果有的話) 作為模板。
    ```env
    DATABASE_URL="postgresql://user:password@host:port/database"
    STEAM_API_KEY="YOUR_STEAM_API_KEY"
    TWITCH_CLIENT_ID="YOUR_TWITCH_CLIENT_ID"
    TWITCH_CLIENT_SECRET="YOUR_TWITCH_CLIENT_SECRET"
    SCRAPERAPI_KEY="YOUR_SCRAPERAPI_KEY"
    ```

6.  **運行爬蟲**:
    ```bash
    python runner.py
    ```

## 專案藍圖 (Project Roadmap)

好奇這些數據能產生什麼商業價值嗎？請查看我們的專案規劃書，了解從數據驗證到打造互動式數據產品的完整藍圖。

> **VISUALIZATION_PLAN.md**