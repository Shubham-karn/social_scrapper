import aioredis
import asyncpg
import logging
import asyncio
import dotenv
import os
import re
import requests
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

dotenv.load_dotenv()

redis = None
mysql_pool = None

df = pd.read_csv('scraped_data_instagram.csv')

def extract_image_id(url):
    match = re.search(r'/(\d+)\.jpg', url)
    return match.group(1) if match else None

df['image_id'] = df['img'].apply(extract_image_id)

image_urls = dict(zip(df['image_id'].dropna(), df['img'][df['image_id'].notna()]))

client = boto3.client(
    's3',
    endpoint_url= os.getenv('S3_ENDPOINT_URL'),
    aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name= os.getenv('AWS_REGION'),
    config=boto3.session.Config(s3={'addressing_style': 'path'})
)

def save_image():
    try:
        for key, value in image_urls.items():
            
            try:
                response = requests.get(value, stream=True)
                response.raise_for_status()
                
                if 'image' in response.headers['Content-Type'] and int(response.headers.get('Content-Length', 0)) > 0:
                    client.put_object(
                        Bucket= os.getenv('BUCKET_NAME'), 
                        Key=f"{key}.jpg",
                        Body=response.content,
                        ContentType=response.headers['Content-Type']
                    )
            except requests.exceptions.RequestException as e:
                print(f"Error downloading image {key}: {e}")
            except Exception as e:
                print(f"Error saving image {key}: {e}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
    except Exception as e:
        print(f"Error: {e}")


async def get_redis():
    return await aioredis.create_redis_pool(
        'redis://redis_container:6379'
    )

async def get_mysql_pool():
    global mysql_pool
    if mysql_pool is None:
        retry_count = 10
        while retry_count > 0:
            try:
                logging.info("Creating MySQL connection pool...")
                mysql_pool = await asyncpg.create_pool(
                    host= os.getenv('DB_HOST'),
                    port= os.getenv('DB_PORT'),
                    user= os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    database=os.getenv('DB_NAME'),
                    statement_cache_size=0
                )
                logging.info("MySQL connection pool created.")
                break
            except Exception as e:
                logging.error(f"Failed to connect to MySQL: {e}. Retrying in 5 seconds...")
                retry_count -= 1
                await asyncio.sleep(5)
        if mysql_pool is None:
            raise Exception("Could not establish a connection to MySQL after multiple attempts.")
    return mysql_pool

async def create_insta_table(mysql_pool):
    create_instagram_stats_sql = """
    CREATE TABLE IF NOT EXISTS instagram_stats (
        ID SERIAL PRIMARY KEY,
        Username VARCHAR(255) NOT NULL UNIQUE,
        Category VARCHAR(255),
        Country VARCHAR(255),
        ImageURL VARCHAR(255)
    );
    """

    create_followers_insta_sql = """
    CREATE TABLE IF NOT EXISTS followers_insta (
        ID SERIAL PRIMARY KEY,
        InstagramStatsID INT,
        FollowersCount VARCHAR(255) NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (InstagramStatsID) REFERENCES instagram_stats(ID)
    );
    """

    create_engagement_history_sql = """
    CREATE TABLE IF NOT EXISTS engagement_history (
        ID SERIAL PRIMARY KEY,
        InstagramStatsID INT,
        EngagementRate VARCHAR(255) NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (InstagramStatsID) REFERENCES instagram_stats(ID)
    );
    """

    create_rank_insta_sql = """
    CREATE TABLE IF NOT EXISTS rank_insta (
        ID SERIAL PRIMARY KEY,
        InstagramStatsID INT,
        Position INT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (InstagramStatsID) REFERENCES instagram_stats(ID)
    );
    """

    try:
        async with mysql_pool.acquire() as conn:
            await conn.execute(create_instagram_stats_sql)
            await conn.execute(create_followers_insta_sql)
            await conn.execute(create_engagement_history_sql)
            await conn.execute(create_rank_insta_sql)
            logging.info("Instagram tables created successfully.")
    except Exception as e:
        logging.error(f"Failed to create Instagram tables: {e}")

async def create_tiktok_tables(pg_pool):
    create_tiktok_stats_sql = """
    CREATE TABLE IF NOT EXISTS tiktok_stats (
        ID SERIAL PRIMARY KEY,
        Username VARCHAR(255) NOT NULL UNIQUE,
        ImageURL VARCHAR(255)
    );
    """

    create_comments_history_sql = """
    CREATE TABLE IF NOT EXISTS comments_history (
        ID SERIAL PRIMARY KEY,
        TikTokStatsID INT,
        CommentsCount VARCHAR(255) NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_followers_tiktok_sql = """
    CREATE TABLE IF NOT EXISTS followers_tiktok (
        ID SERIAL PRIMARY KEY,
        TikTokStatsID INT,
        FollowersCount VARCHAR(255) NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_likes_history_sql = """
    CREATE TABLE IF NOT EXISTS likes_history (
        ID SERIAL PRIMARY KEY,
        TikTokStatsID INT,
        LikesCount VARCHAR(255) NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_views_history_sql = """
    CREATE TABLE IF NOT EXISTS views_history (
        ID SERIAL PRIMARY KEY,
        TikTokStatsID INT,
        ViewsCount VARCHAR(255) NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_shares_history_sql = """
    CREATE TABLE IF NOT EXISTS shares_history (
        ID SERIAL PRIMARY KEY,
        TikTokStatsID INT,
        SharesCount VARCHAR(255) NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_rank_tiktok_sql = """
    CREATE TABLE IF NOT EXISTS rank_tiktok (
        ID SERIAL PRIMARY KEY,
        TikTokStatsID INT,
        Position INT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    try:
        async with pg_pool.acquire() as conn:
            await conn.execute(create_tiktok_stats_sql)
            await conn.execute(create_comments_history_sql)
            await conn.execute(create_followers_tiktok_sql)
            await conn.execute(create_likes_history_sql)
            await conn.execute(create_views_history_sql)
            await conn.execute(create_shares_history_sql)
            await conn.execute(create_rank_tiktok_sql)
            logging.info("TikTok tables created successfully.")
    except Exception as e:
        logging.error(f"Failed to create TikTok tables: {e}")

# async def main():
#     global redis, mysql_pool
#     redis = await get_redis()
#     mysql_pool = await get_mysql_pool()
#     await create_insta_table(mysql_pool)
#     await create_tiktok_tables(mysql_pool)

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     asyncio.run(main())
