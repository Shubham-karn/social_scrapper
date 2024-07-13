import logging
import pandas as pd
import aiomysql

async def query_insta_user_data(mysql_pool, username):
    query_sql = """
    SELECT 
        i.Username, i.Category, i.Country, i.ImageURL, 
        f.FollowersCount, f.RecordedAt AS FollowersRecordedAt,
        e.EngagementRate, e.RecordedAt AS EngagementRecordedAt,
        r.Position, r.RecordedAt AS RankRecordedAt
    FROM instagram_stats i
    LEFT JOIN followers_insta f ON i.ID = f.InstagramStatsID
    LEFT JOIN engagement_history e ON i.ID = e.InstagramStatsID
    LEFT JOIN rank_insta r ON i.ID = r.InstagramStatsID
    WHERE i.Username = %s
    ORDER BY f.RecordedAt DESC, e.RecordedAt DESC;
    """
    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query_sql, (username,))
                result = await cur.fetchall()

                df = pd.DataFrame(result)

                nested_df = df.groupby(["Username", "Category", "Country", "ImageURL"]).apply(lambda x: x[["Position", "FollowersCount", "FollowersRecordedAt", "EngagementRate", "EngagementRecordedAt", "RankRecordedAt"]].to_dict(orient="records")).reset_index()
                nested_df.columns = ["Username", "Category", "Country", "ImageURL", "Details"]

                return nested_df.to_dict(orient="records")
            
    except Exception as e:
        logging.error(f"Failed to query Instagram user data: {e}")
        return None

async def query_tiktok_user_data(mysql_pool, username):
    query_sql = """
    SELECT 
        t.Username, t.ImageURL, r.Position,
        c.CommentsCount, ft.FollowersCount, l.LikesCount, v.ViewsCount, s.SharesCount, 
        c.RecordedAt AS CommentsRecordedAt, ft.RecordedAt AS FollowersRecordedAt, 
        l.RecordedAt AS LikesRecordedAt, v.RecordedAt AS ViewsRecordedAt, s.RecordedAt AS SharesRecordedAt
    FROM tiktok_stats t
    LEFT JOIN comments_history c ON t.ID = c.TikTokStatsID
    LEFT JOIN followers_tiktok ft ON t.ID = ft.TikTokStatsID
    LEFT JOIN likes_history l ON t.ID = l.TikTokStatsID
    LEFT JOIN views_history v ON t.ID = v.TikTokStatsID
    LEFT JOIN shares_history s ON t.ID = s.TikTokStatsID
    LEFT JOIN rank_tiktok r ON t.ID = r.TikTokStatsID
    WHERE t.Username = %s;
    """
    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query_sql, (username,))
                result = await cur.fetchall()

                df = pd.DataFrame(result)

                nested_df = df.groupby(["Username", "ImageURL"]).apply(lambda x: x[["Position", "CommentsCount", "CommentsRecordedAt", "FollowersCount", "FollowersRecordedAt", "LikesCount", "LikesRecordedAt", "ViewsCount", "ViewsRecordedAt", "SharesCount", "SharesRecordedAt"]].to_dict(orient="records")).reset_index()
                nested_df.columns = ["Username", "ImageURL", "Details"]

                return nested_df.to_dict(orient="records")
            
    except Exception as e:
        logging.error(f"Failed to query TikTok user data: {e}")
        return None    

async def update_or_insert_instagram_data_from_csv(mysql_pool, csv_file_path):
    check_user_sql = """
    SELECT ID FROM instagram_stats WHERE Username = %s;
    """

    insert_user_sql = """
    INSERT INTO instagram_stats (Username, Category, Country, ImageURL)
    VALUES (%s, %s, %s, %s);
    """

    update_stats_sql = """
    UPDATE instagram_stats
    SET Category = %s, Country = %s, ImageURL = %s
    WHERE Username = %s;
    """

    insert_rank_sql = """
    INSERT INTO rank_insta (InstagramStatsID, Position, RecordedAt)
    VALUES ((SELECT ID FROM instagram_stats WHERE Username = %s), %s, NOW());
    """

    insert_followers_sql = """
    INSERT INTO followers_insta (InstagramStatsID, FollowersCount, RecordedAt)
    VALUES ((SELECT ID FROM instagram_stats WHERE Username = %s), %s, NOW());
    """

    insert_engagement_sql = """
    INSERT INTO engagement_history (InstagramStatsID, EngagementRate, RecordedAt)
    VALUES ((SELECT ID FROM instagram_stats WHERE Username = %s), %s, NOW());
    """

    df = pd.read_csv(csv_file_path)
    df = df.where(pd.notnull(df), None)

    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                for index, row in df.iterrows():
                    # Check if the user exists
                    await cur.execute(check_user_sql, (row['Username'],))
                    user_exists = await cur.fetchone()

                    if not user_exists:
                        # Insert new user if not exists
                        await cur.execute(insert_user_sql, (
                            row['Username'], row['Category'], row['Country'], row['img']
                        ))

                    # Update instagram_stats excluding Rank
                    await cur.execute(update_stats_sql, (
                        row['Category'], row['Country'], row['img'], row['Username']
                    ))

                    # Insert new rank_insta record with current timestamp
                    await cur.execute(insert_rank_sql, (
                        row['Username'], row['Rank']
                    ))

                    # Insert new followers_insta record with current timestamp
                    await cur.execute(insert_followers_sql, (
                        row['Username'], row['Followers']
                    ))

                    # Insert new engagement_history record with current timestamp
                    await cur.execute(insert_engagement_sql, (
                        row['Username'], row['Engagement']
                    ))

                await conn.commit()
                logging.info("Instagram data updated successfully from CSV.")
    except Exception as e:
        logging.error(f"Failed to update or insert Instagram data from CSV: {e}")

async def update_or_insert_tiktok_data_from_csv(mysql_pool, csv_file_path):
    check_user_sql = """
    SELECT ID FROM tiktok_stats WHERE Username = %s;
    """

    insert_user_sql = """
    INSERT INTO tiktok_stats (Username, ImageURL)
    VALUES (%s, %s);
    """

    update_stats_sql = """
    UPDATE tiktok_stats
    SET ImageURL = %s
    WHERE Username = %s;
    """

    insert_rank_sql = """
    INSERT INTO rank_tiktok (TikTokStatsID, Position, RecordedAt)
    VALUES ((SELECT ID FROM tiktok_stats WHERE Username = %s), %s, NOW());
    """

    insert_comments_sql = """
    INSERT INTO comments_history (TikTokStatsID, CommentsCount, RecordedAt)
    VALUES ((SELECT ID FROM tiktok_stats WHERE Username = %s), %s, NOW());
    """

    insert_followers_sql = """
    INSERT INTO followers_tiktok (TikTokStatsID, FollowersCount, RecordedAt)
    VALUES ((SELECT ID FROM tiktok_stats WHERE Username = %s), %s, NOW());
    """

    insert_likes_sql = """
    INSERT INTO likes_history (TikTokStatsID, LikesCount, RecordedAt)
    VALUES ((SELECT ID FROM tiktok_stats WHERE Username = %s), %s, NOW());
    """

    insert_views_sql = """
    INSERT INTO views_history (TikTokStatsID, ViewsCount, RecordedAt)
    VALUES ((SELECT ID FROM tiktok_stats WHERE Username = %s), %s, NOW());
    """

    insert_shares_sql = """
    INSERT INTO shares_history (TikTokStatsID, SharesCount, RecordedAt)
    VALUES ((SELECT ID FROM tiktok_stats WHERE Username = %s), %s, NOW());
    """

    df = pd.read_csv(csv_file_path)
    df = df.where(pd.notnull(df), None)

    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                for index, row in df.iterrows():
                    # Check if the user exists
                    await cur.execute(check_user_sql, (row['Username'],))
                    user_exists = await cur.fetchone()

                    if not user_exists:
                        # Insert new user if not exists
                        await cur.execute(insert_user_sql, (
                            row['Username'], row['img']
                        ))

                    # Update tiktok_stats excluding Rank
                    await cur.execute(update_stats_sql, (
                        row['img'], row['Username']
                    ))

                    # Insert new rank_tiktok record with current timestamp
                    await cur.execute(insert_rank_sql, (
                        row['Username'], row['Rank']
                    ))

                    # Insert new comments_history record with current timestamp
                    await cur.execute(insert_comments_sql, (
                        row['Username'], row['Comments']
                    ))

                    # Insert new followers_tiktok record with current timestamp
                    await cur.execute(insert_followers_sql, (
                        row['Username'], row['Followers']
                    ))

                    # Insert new likes_history record with current timestamp
                    await cur.execute(insert_likes_sql, (
                        row['Username'], row['Likes']
                    ))

                    # Insert new views_history record with current timestamp
                    await cur.execute(insert_views_sql, (
                        row['Username'], row['Views']
                    ))

                    # Insert new shares_history record with current timestamp
                    await cur.execute(insert_shares_sql, (
                        row['Username'], row['Shares']
                    ))

                await conn.commit()
                logging.info("TikTok data updated successfully from CSV.")
    except Exception as e:
        logging.error(f"Failed to update or insert TikTok data from CSV: {e}")
