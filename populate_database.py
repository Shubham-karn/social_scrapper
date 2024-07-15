import pandas as pd
import asyncio
import asyncpg
import os
import dotenv

dotenv.load_dotenv()

# Load CSV files
df_insta = pd.read_csv('./scraped_data_instagram.csv')
df_tiktok = pd.read_csv('./scraped_data_tiktok.csv')

# Fill missing values
df_insta.fillna(value={'Username': 'N/A', 'Category': 'N/A', 'Country': 'N/A', 'img': 'N/A'}, inplace=True)
df_tiktok.fillna(value={'Username': 'N/A', 'img': 'N/A'}, inplace=True)

async def populate_insta_database():
    conn = await asyncpg.connect(
        user=os.getenv('DB_USER'), 
        password=os.getenv('DB_PASSWORD'), 
        host=os.getenv('DB_HOST'), 
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        statement_cache_size=0  # Disable statement cache
    )
    try:
        for index, row in df_insta.iterrows():
            # Insert or update Instagram stats
            await conn.execute("""
                INSERT INTO instagram_stats (Username, Category, Country, ImageURL)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (Username) DO UPDATE 
                SET Category = EXCLUDED.Category, 
                    Country = EXCLUDED.Country, 
                    ImageURL = EXCLUDED.ImageURL
            """, row['Username'], row['Category'], row['Country'], row['img'])

            # Get the Instagram stats ID
            instagram_stats_id = await conn.fetchval(
                "SELECT ID FROM instagram_stats WHERE Username = $1", 
                row['Username']
            )

            if instagram_stats_id:
                # Insert followers count
                await conn.execute("""
                    INSERT INTO followers_insta (InstagramStatsID, FollowersCount, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, instagram_stats_id, row['Followers'])

                # Insert engagement rate
                await conn.execute("""
                    INSERT INTO engagement_history (InstagramStatsID, EngagementRate, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, instagram_stats_id, row['Engagement'])

                # Insert rank
                await conn.execute("""
                    INSERT INTO rank_insta (InstagramStatsID, Position, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, instagram_stats_id, row['Rank'])

    finally:
        await conn.close()

async def populate_tiktok_database():
    conn = await asyncpg.connect(
        user=os.getenv('DB_USER'), 
        password=os.getenv('DB_PASSWORD'), 
        host=os.getenv('DB_HOST'), 
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        statement_cache_size=0  # Disable statement cache
    )
    try:
        for index, row in df_tiktok.iterrows():
            # Insert or update TikTok stats
            await conn.execute("""
                INSERT INTO tiktok_stats (Username, ImageURL)
                VALUES ($1, $2)
                ON CONFLICT (Username) DO UPDATE 
                SET ImageURL = EXCLUDED.ImageURL
            """, row['Username'], row['img'])

            # Get the TikTok stats ID
            tiktok_stats_id = await conn.fetchval(
                "SELECT ID FROM tiktok_stats WHERE Username = $1", 
                row['Username']
            )

            if tiktok_stats_id:
                # Insert comments count
                await conn.execute("""
                    INSERT INTO comments_history (TikTokStatsID, CommentsCount, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, tiktok_stats_id, row['Comments'])

                # Insert followers count
                await conn.execute("""
                    INSERT INTO followers_tiktok (TikTokStatsID, FollowersCount, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, tiktok_stats_id, row['Followers'])

                # Insert likes count
                await conn.execute("""
                    INSERT INTO likes_history (TikTokStatsID, LikesCount, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, tiktok_stats_id, row['Likes'])

                # Insert views count
                await conn.execute("""
                    INSERT INTO views_history (TikTokStatsID, ViewsCount, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, tiktok_stats_id, row['Views'])

                # Insert shares count
                await conn.execute("""
                    INSERT INTO shares_history (TikTokStatsID, SharesCount, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, tiktok_stats_id, row['Shares'])

                # Insert rank
                await conn.execute("""
                    INSERT INTO rank_tiktok (TikTokStatsID, Position, RecordedAt)
                    VALUES ($1, $2, NOW())
                """, tiktok_stats_id, row['Rank'])

    finally:
        await conn.close()

# Run the main functions to populate databases
asyncio.run(populate_tiktok_database())
asyncio.run(populate_insta_database())
