# MySQL 資料庫遷移腳本

此專案是一個利用 Python 的 asyncio 與 aiomysql 庫實作的 MySQL 資料庫遷移工具，可用來將來源資料庫的表結構與數據批次搬移到目標資料庫，並自動將文字型欄位轉換為 utf8mb4 與 utf8mb4_general_ci 編碼格式。

## 功能

- **表結構複製**  
  使用 `SHOW CREATE TABLE` 取得來源表的建表語法，並在目標資料庫上先 DROP 再 CREATE，避免因重複建立而發生錯誤。  
  同時，透過正則表達式自動將所有支援字符集的欄位修改成 `utf8mb4` 與 `utf8mb4_general_ci`。

- **資料分批搬移**  
  為避免記憶體過載，搬移數據採用分批讀取（預設批次大小 10000 筆）與插入的方式，並在過程中印出進度訊息。

- **異步處理**  
  全部流程皆採用 asyncio 非同步方式進行，提升大資料量遷移時的效率。

## 環境配置

1. **安裝相依套件**  
   使用 pip 安裝所需的 Python 套件：
   ```bash
   pip install aiomysql python-dotenv asyncio
   ```
2. **指定env**
    於最上方 load_dotenv(".檔名")
    ```
    #來源DB
    SOURCE_DB_HOST=your_source_host
    SOURCE_DB_USER=your_source_user
    SOURCE_DB_PASSWORD=your_source_password
    SOURCE_DB_NAME=your_source_db

    目標DB
    TARGET_DB_HOST=your_target_host
    TARGET_DB_USER=your_target_user
    TARGET_DB_PASSWORD=your_target_password
    TARGET_DB_NAME=your_target_db
    ```
3. 直接執行就能搬遷 python main.py

## 部分修改
- load_dotenv(".檔名") 可選自己需要的檔案
- 修改tables可以指定搬遷表名
- await insert_data(target_pool, table, data, total_rows) 不執行就是只拉結構不拉資料
- CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci 也能調整