# Steam 市場洞察數據管道 (Steam Market Insight Data Pipeline)

一個生產級、雲端原生的自動化數據管道，旨在每日從 Steam 和 Twitch 平台採集市場數據。此專案不僅是一個數據抓取工具，更是一個展現了高韌性、可擴展性與現代雲端部署實踐的完整數據工程解決方案。

---

## 核心技術亮點 (Key Features)

*   **端到端自動化 (End-to-End Automation)**: 每日自動執行，從數據採集、清洗、整合到儲存，無需人工干預。
*   **高韌性系統設計 (High-Resilience Design)**:
    *   **智慧重試**: 針對網路波動與 API 錯誤，實現了帶有指數退避 (Exponential Backoff) 與隨機抖動 (Jitter) 的自動重試機制。
    *   **優雅關閉**: 系統能響應 `SIGTERM` 信號，確保在雲端平台重啟或部署時，能安全地完成當前任務，防止數據損壞。
    *   **分布式鎖**: 透過資料庫實現的鎖定機制，確保在任何情況下都只有一個爬蟲實例在運行，防止競爭條件與數據污染。
*   **進階反爬蟲策略 (Advanced Anti-Scraping)**:
    *   **代理整合**: 透過 ScraperAPI 整合代理服務，並預留了升級至住宅 IP 的開關。
    *   **擬人化模擬**: 透過輪換 User-Agent 和在請求間加入隨機延遲，模擬真實用戶行為，大幅提升抓取成功率。
*   **雲端原生架構 (Cloud-Native Architecture)**:
    *   **容器化思維**: 專為在 Render 等 PaaS 平台上作為背景工人 (Background Worker) 部署而設計。
    *   **環境變數驅動**: 所有敏感金鑰與配置均透過環境變數管理，符合十二因子應用 (Twelve-Factor App) 規範。
*   **高效能數據處理 (High-Performance Data Handling)**:
    *   **異步 I/O**: 全面採用 `asyncio` 和 `httpx` 進行高並發的網路請求。
    *   **批次處理**: 數據以批次方式處理，並透過 SQLAlchemy Core 的 `pg_insert` 實現高效的 "Upsert" 操作，最大化資料庫寫入效能。

## 系統架構 (System Architecture)

本專案由兩個核心雲端服務組成：

1.  **PostgreSQL 資料庫 (`steam-db`)**:
    *   **平台**: Render
    *   **作用**: 作為數據的持久化儲存層。包含 `games_metadata`（靜態元數據）、`games_timeseries`（動態時間序列數據）和 `scraping_state`（用於實現分布式鎖）三個核心資料表。

2.  **背景工人 (`steam-scraper-worker`)**:
    *   **平台**: Render
    *   **作用**: 核心的數據處理服務。它是一個無狀態的 Python 應用，由 `runner.py` 啟動，每日在隨機的排定時間觸發 `main.py` 中的 `scrape_and_store_data` 函數，執行完整的數據管道任務。

## 技術棧 (Technology Stack)

*   **語言**: Python 3.9+
*   **核心框架**: `asyncio`
*   **網路請求**: `httpx`
*   **網頁解析**: `BeautifulSoup4`
*   **資料庫 ORM**: `SQLAlchemy`
*   **雲端平台**: Render
*   **資料庫**: PostgreSQL
*   **代理服務**: ScraperAPI

## 專案文件

*   **程式碼導覽**:
    *   需要將此專案部署到您自己的 Render 帳號嗎？請遵循 `RENDER_DEPLOYMENT_GUIDE.txt` 中的詳細步驟。

*   **視覺化與分析規劃**:
    *   好奇這些數據能產生什麼商業價值嗎？請查看 `VISUALIZATION_PLAN.md`，了解我們從數據驗證到互動式產品開發的完整藍圖。