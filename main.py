import os
import asyncio
import aiomysql
from dotenv import load_dotenv
import re
# 讀取 .env 檔案 決定目標與來源
load_dotenv(".env_A")

# MySQL 連線資訊
SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD")
SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME")

TARGET_DB_HOST = os.getenv("TARGET_DB_HOST")
TARGET_DB_USER = os.getenv("TARGET_DB_USER")
TARGET_DB_PASSWORD = os.getenv("TARGET_DB_PASSWORD")
TARGET_DB_NAME = os.getenv("TARGET_DB_NAME")

# 設定批次大小（避免記憶體爆炸）
BATCH_SIZE = 10000


async def get_all_tables(source_pool):
    """ 取得來源資料庫的所有表 """
    async with source_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in await cursor.fetchall()]
            print(f"[INFO] 發現 {len(tables)} 個表: {tables}")
            return tables


async def replicate_table_schema(source_pool, target_pool, table_name):
    """
    從來源資料庫讀取該表的 CREATE TABLE 語法，
    再在目標資料庫 DROP TABLE IF EXISTS，最後執行 CREATE TABLE，
    並強制轉換該表中所有支援字符集的欄位（包含所有文字型欄位）為 utf8mb4 與 utf8mb4_general_ci。
    ※ 確保只會在目標上操作，避免誤砍來源。
    """
    # 1. 從來源資料庫取得 `CREATE TABLE` 語法
    async with source_pool.acquire() as source_conn:
        async with source_conn.cursor() as source_cursor:
            await source_cursor.execute(f"SHOW CREATE TABLE `{table_name}`;")
            result = await source_cursor.fetchone()
            create_sql = result[1]
            # 將來源資料庫名稱移除，避免語法中含有 `source_db`.`table_name`
            create_sql = create_sql.replace(f"`{SOURCE_DB_NAME}`.", "")
            # 指定預設的編碼與校對集為 utf8mb4 與 utf8mb4_general_ci
            if "DEFAULT CHARSET" in create_sql.upper():
                create_sql = re.sub(r"(?i)DEFAULT CHARSET=\w+", "DEFAULT CHARSET=utf8mb4", create_sql)
                create_sql = re.sub(r"(?i)COLLATE=\w+", "COLLATE=utf8mb4_general_ci", create_sql)
            else:
                create_sql = create_sql.rstrip(";") + " DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;"

    # 2. 在目標資料庫先 DROP，再 CREATE
    async with target_pool.acquire() as target_conn:
        async with target_conn.cursor() as target_cursor:
            # 強制指定目標資料庫
            drop_sql = f"DROP TABLE IF EXISTS `{TARGET_DB_NAME}`.`{table_name}`;"
            await target_cursor.execute(drop_sql)

            # 指定使用目標資料庫
            use_sql = f"USE `{TARGET_DB_NAME}`;"
            await target_cursor.execute(use_sql)

            # 建立表結構
            await target_cursor.execute(create_sql)
            await target_conn.commit()

            # 3. 強制轉換該表中所有支援字符集的欄位
            # ALTER TABLE ... CONVERT TO 將會轉換該表中所有文字型欄位
            alter_sql = (
                f"ALTER TABLE `{TARGET_DB_NAME}`.`{table_name}` "
                "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"
            )
            await target_cursor.execute(alter_sql)
            await target_conn.commit()
            print(f"[INFO] `{table_name}` 建表並強制轉換所有文字欄位完成")



async def fetch_data(source_pool, table_name):
    """ 分批讀取來源表數據，顯示進度 """
    async with source_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(f"SELECT COUNT(1) FROM `{table_name}`")
            total_rows = (await cursor.fetchone())["COUNT(1)"]
            print(f"[INFO] `{table_name}` 共 {total_rows} 筆數據，開始搬移...")

            data = []
            for offset in range(0, total_rows, BATCH_SIZE):
                await cursor.execute(
                    f"SELECT * FROM `{table_name}` LIMIT {BATCH_SIZE} OFFSET {offset}"
                )
                batch = await cursor.fetchall()

                percent_complete = round((offset + len(batch)) / total_rows * 100, 2)
                print(f"[INFO] `{table_name}` 讀取 {len(batch)} 筆數據 ({percent_complete}%)...")

                data.extend(batch)

            return data, total_rows


async def insert_data(target_pool, table_name, data, total_rows):
    """ 插入數據到目標資料庫，顯示進度 """
    if not data:
        print(f"[INFO] `{table_name}` 無可搬移數據")
        return

    async with target_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            columns = ", ".join(f"`{col}`" for col in data[0].keys())
            placeholders = ", ".join(["%s"] * len(data[0]))
            insert_sql = f"INSERT INTO `{TARGET_DB_NAME}`.`{table_name}` ({columns}) VALUES ({placeholders})"

            for index, row in enumerate(data, start=1):
                # 把 "0000-00-00" 轉成 None
                for key, value in row.items():
                    if isinstance(value, str) and value == "0000-00-00":
                        row[key] = None

                await cursor.execute(insert_sql, tuple(row.values()))

                if index % 1000 == 0 or index == total_rows:
                    percent_complete = round(index / total_rows * 100, 2)
                    print(f"[INFO] `{table_name}` 插入進度: {percent_complete}% ({index}/{total_rows})")

            await conn.commit()
            print(f"[INFO] `{table_name}` 搬移完成，共 {total_rows} 筆數據")


async def migrate_database():
    """ 主函式：從來源資料庫搬移到目標資料庫 """
    try:
        # 建立 MySQL 連線池
        source_pool = await aiomysql.create_pool(
            host=SOURCE_DB_HOST, user=SOURCE_DB_USER, password=SOURCE_DB_PASSWORD,
            db=SOURCE_DB_NAME, charset='utf8mb4'
        )
        target_pool = await aiomysql.create_pool(
            host=TARGET_DB_HOST, user=TARGET_DB_USER, password=TARGET_DB_PASSWORD,
            db=TARGET_DB_NAME, charset='utf8mb4'
        )
        print("[INFO] MySQL 連線池建立完成")

        # 取得所有表 (來源資料庫)
        tables = await get_all_tables(source_pool)
        #tables = ["Atable"] # 指定輸出 
        
        # 逐表搬移
        for table in tables:
            # 1. 先複製表結構到目標（如果已存在就砍掉後重建）
            await replicate_table_schema(source_pool, target_pool, table)
            # 2. 抓取來源資料
            data, total_rows = await fetch_data(source_pool, table)
            # 3. 寫入目標資料庫
            await insert_data(target_pool, table, data, total_rows)

    except Exception as e:
        print(f"[ERROR] 發生錯誤: {e}")
    finally:
        source_pool.close()
        await source_pool.wait_closed()
        target_pool.close()
        await target_pool.wait_closed()
        print("[INFO] 所有數據搬移完成")


if __name__ == "__main__":
    asyncio.run(migrate_database())
