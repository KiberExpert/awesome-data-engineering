import os
from snowflake.connector import connect
from dotenv import load_dotenv

load_dotenv()

def upload_to_snowflake(df, table_name):
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cs = conn.cursor()
    try:
        # Ustun nomlarini STRING turida yaratish
        columns_with_types = ", ".join(f"{col} STRING" for col in df.columns)
        cs.execute(f"CREATE OR REPLACE TABLE {table_name} ({columns_with_types})")

        # Ma'lumotlarni kiritish
        for _, row in df.iterrows():
            values = "', '".join(str(x).replace("'", "''") for x in row.tolist())  # SQL injection oldini olish
            cs.execute(f"INSERT INTO {table_name} VALUES ('{values}')")
    finally:
        cs.close()
        conn.close()
