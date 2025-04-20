import snowflake.connector
import csv
import os

from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)



cursor = conn.cursor()

# Open CSV and insert row by row
with open('youtube_data.csv', mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        cursor.execute("""
            INSERT INTO youtube_videos (
                channel_id, video_id, title, published_at,
                view_count, like_count, comment_count, description
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['channel_id'],
            row['video_id'],
            row['title'],
            row['published_at'],
            int(row['view_count']),
            int(row['like_count']),
            int(row['comment_count']),
            row['description']
        ))

print("Data ingested into Snowflake ✅")

cursor.close()
conn.close()
