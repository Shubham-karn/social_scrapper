import aioredis
import aiomysql
import logging
import asyncio

redis = None
mysql_pool = None

async def get_redis():
    return await aioredis.create_redis_pool(
        'redis://redis_container:6379', encoding='utf8'
    )

async def get_mysql_pool():
    global mysql_pool
    if mysql_pool is None:
        retry_count = 10
        while retry_count > 0:
            try:
                logging.info("Creating MySQL connection pool...")
                mysql_pool = await aiomysql.create_pool(
                    host='mysql_container',
                    port=3306,
                    user='root',
                    password='root',
                    db='summarizer',
                    charset='utf8',
                    autocommit=True,
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
        ID INT AUTO_INCREMENT PRIMARY KEY,
        Username VARCHAR(255) NOT NULL UNIQUE,
        Category VARCHAR(255) NOT NULL,
        Country VARCHAR(255),
        ImageURL VARCHAR(255),
        `Rank` INT
    );
    """

    create_followers_history_sql = """
    CREATE TABLE IF NOT EXISTS followers_history (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        InstagramStatsID INT,
        FollowersCount INT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (InstagramStatsID) REFERENCES instagram_stats(ID)
    );
    """

    create_engagement_history_sql = """
    CREATE TABLE IF NOT EXISTS engagement_history (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        InstagramStatsID INT,
        EngagementRate FLOAT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (InstagramStatsID) REFERENCES instagram_stats(ID)
    );
    """

    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_instagram_stats_sql)
                await cur.execute(create_followers_history_sql)
                await cur.execute(create_engagement_history_sql)
                await conn.commit()
                logging.info("Tables created successfully.")
    except Exception as e:
        logging.error(f"Failed to create tables: {e}")

async def create_tiktok_tables(mysql_pool):
    create_tiktok_stats_sql = """
    CREATE TABLE IF NOT EXISTS tiktok_stats (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        Username VARCHAR(255) NOT NULL UNIQUE,
        img VARCHAR(255),
        `Rank` INT
    );
    """

    create_comments_history_sql = """
    CREATE TABLE IF NOT EXISTS comments_history (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        TikTokStatsID INT,
        CommentsCount INT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_followers_history_sql = """
    CREATE TABLE IF NOT EXISTS followers_history (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        TikTokStatsID INT,
        FollowersCount INT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_likes_history_sql = """
    CREATE TABLE IF NOT EXISTS likes_history (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        TikTokStatsID INT,
        LikesCount INT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_views_history_sql = """
    CREATE TABLE IF NOT EXISTS views_history (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        TikTokStatsID INT,
        ViewsCount INT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    create_shares_history_sql = """
    CREATE TABLE IF NOT EXISTS shares_history (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        TikTokStatsID INT,
        SharesCount INT NOT NULL,
        RecordedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (TikTokStatsID) REFERENCES tiktok_stats(ID)
    );
    """

    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_tiktok_stats_sql)
                await cur.execute(create_comments_history_sql)
                await cur.execute(create_followers_history_sql)
                await cur.execute(create_likes_history_sql)
                await cur.execute(create_views_history_sql)
                await cur.execute(create_shares_history_sql)
                await conn.commit()
                logging.info("TikTok stats and history tables created successfully.")
    except Exception as e:
        logging.error(f"Failed to create TikTok tables: {e}")