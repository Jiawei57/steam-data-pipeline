# ======================================================================================
# Steam 市場洞察數據管道 Dockerfile
#
# 本 Dockerfile 利用多階段建置 (multi-stage build) 策略，
# 來打造一個體積小、安全性高且高效率的生產環境映像檔。
# ======================================================================================

# --- 基礎階段 (Base Stage) ---
# 定義後續階段的基礎映像檔。
# 使用特定版本 ('3.11-slim-bullseye') 可確保環境的可重現性與安全性。
# 'slim' 版本提供了最小化的運行環境，能有效減小映像檔體積和攻擊面。
FROM python:3.11-slim-bullseye AS base

# --- 環境變數 ---
# PYTHONDONTWRITEBYTECODE: 防止 Python 寫入 .pyc 檔案，這在容器中是不必要的。
# PYTHONUNBUFFERED: 確保 Python 的輸出（如日誌）直接發送到終端，不進行緩衝，
# 這對於在容器編排工具中進行即時日誌監控至關重要。
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# --- 安裝系統依賴 ---
# 安裝 git，以便應用程式可以讀取 commit 訊息來控制啟動行為。
# `--no-install-recommends` 可以減少不必要的套件安裝，保持映像檔精簡。
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# 設定後續指令的預設工作目錄。
WORKDIR /app

# --- 安全性最佳實踐：非 Root 使用者 ---
# 建立一個專用的非 root 使用者來運行應用程式。
# 這遵循了「最小權限原則」，能顯著降低當應用程式進程被入侵時的風險。
RUN useradd --system --uid 1000 --gid 0 -m -s /bin/bash appuser

# --- 依賴安裝階段 (Dependencies Stage) ---
# 此階段專門用於安裝 Python 依賴。
# 透過分離依賴安裝步驟，我們可以充分利用 Docker 的層快取機制。
# 當應用程式碼變更時，無需重新安裝所有依賴，從而加快建置速度。
FROM base AS dependencies

# 先僅複製 requirements.txt 檔案，以最大化利用快取。
COPY --chown=appuser:0 requirements.txt .

# 安裝依賴。`--no-cache-dir` 選項可以減小映像檔體積。
RUN pip install --no-cache-dir -r requirements.txt

# --- 應用程式階段 (Application Stage) ---
# 這是最終的、精簡的生產環境映像檔。
FROM base AS application

# 從 'dependencies' 階段複製已安裝的套件到最終映像檔中。
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# 將應用程式原始碼複製到容器中。
COPY --chown=appuser:0 *.py ./

# 在運行應用程式前，切換到非 root 使用者。
USER appuser

# 容器啟動時將執行的指令。
CMD ["python", "runner.py"]