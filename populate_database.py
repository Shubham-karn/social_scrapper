import pandas as pd
import asyncio
import aiomysql

# Load CSV files
df_insta = pd.read_csv('./scraped_data_instagram.csv')
df_tiktok = pd.read_csv('./scraped_data_tiktok.csv')

# Fill missing values
df_insta.fillna(value={'Username': 'N/A', 'Category': 'N/A', 'Country': 'N/A', 'img': 'N/A'}, inplace=True)
df_tiktok.fillna(value={'Username': 'N/A', 'img': 'N/A'}, inplace=True)

async def populate_insta_database():
    conn = await aiomysql.connect(user='root', password='root', host='localhost', port=3306, db='summarizer')
    async with conn.cursor() as cursor:
        for index, row in df_insta.iterrows():
            # Insert or update Instagram stats
            await cursor.execute("""
                INSERT INTO instagram_stats (Username, Category, Country, ImageURL)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Category=VALUES(Category), Country=VALUES(Country), ImageURL=VALUES(ImageURL)
            """, (row['Username'], row['Category'], row['Country'], row['img']))
            await conn.commit()

            # Get the Instagram stats ID
            await cursor.execute("SELECT ID FROM instagram_stats WHERE Username = %s", (row['Username'],))
            instagram_stats_id = await cursor.fetchone()
            if instagram_stats_id:
                instagram_stats_id = instagram_stats_id[0]

                # Insert followers count
                await cursor.execute("""
                    INSERT INTO followers_insta (InstagramStatsID, FollowersCount, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (instagram_stats_id, row['Followers']))
                await conn.commit()

                # Insert engagement rate
                await cursor.execute("""
                    INSERT INTO engagement_history (InstagramStatsID, EngagementRate, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (instagram_stats_id, row['Engagement']))
                await conn.commit()

                # Insert rank
                await cursor.execute("""
                    INSERT INTO rank_insta (InstagramStatsID, Position, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (instagram_stats_id, row['Rank']))
                await conn.commit()

    conn.close()

async def populate_tiktok_database():
    conn = await aiomysql.connect(user='root', password='root', host='localhost', port=3307, db='summarizer')
    async with conn.cursor() as cursor:
        for index, row in df_tiktok.iterrows():
            # Insert or update TikTok stats
            await cursor.execute("""
                INSERT INTO tiktok_stats (Username, ImageURL)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE ImageURL=VALUES(ImageURL)
            """, (row['Username'], row['img']))
            await conn.commit()

            # Get the TikTok stats ID
            await cursor.execute("SELECT ID FROM tiktok_stats WHERE Username = %s", (row['Username'],))
            tiktok_stats_id = await cursor.fetchone()
            if tiktok_stats_id:
                tiktok_stats_id = tiktok_stats_id[0]

                # Insert comments count
                await cursor.execute("""
                    INSERT INTO comments_history (TikTokStatsID, CommentsCount, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (tiktok_stats_id, row['Comments']))
                await conn.commit()

                # Insert followers count
                await cursor.execute("""
                    INSERT INTO followers_tiktok (TikTokStatsID, FollowersCount, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (tiktok_stats_id, row['Followers']))
                await conn.commit()

                # Insert likes count
                await cursor.execute("""
                    INSERT INTO likes_history (TikTokStatsID, LikesCount, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (tiktok_stats_id, row['Likes']))
                await conn.commit()

                # Insert views count
                await cursor.execute("""
                    INSERT INTO views_history (TikTokStatsID, ViewsCount, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (tiktok_stats_id, row['Views']))
                await conn.commit()

                # Insert shares count
                await cursor.execute("""
                    INSERT INTO shares_history (TikTokStatsID, SharesCount, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (tiktok_stats_id, row['Shares']))
                await conn.commit()

                # Insert rank
                await cursor.execute("""
                    INSERT INTO rank_tiktok (TikTokStatsID, Position, RecordedAt)
                    VALUES (%s, %s, NOW())
                """, (tiktok_stats_id, row['Rank']))
                await conn.commit()

    conn.close()

# Run the main functions to populate databases
asyncio.run(populate_tiktok_database())
asyncio.run(populate_insta_database())
