import json
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import pandas as pd

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

async def query_insta_user_data(pg_pool, username):
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
        WHERE i.Username = $1
        ORDER BY f.RecordedAt DESC, e.RecordedAt DESC, r.RecordedAt DESC
    """
    try:
        async with pg_pool.acquire() as conn:
            results = await conn.fetch(query_sql, username)
            
            if results:
                user_data = {
                    "Username": results[0]['username'],
                    "Category": results[0]['category'],
                    "Country": results[0]['country'],
                    "ImageURL": results[0]['imageurl'],
                    "HistoricalData": defaultdict(dict)
                }
                
                for row in results:
                    recorded_dates = [
                        row['followersrecordedat'],
                        row['engagementrecordedat'],
                        row['rankrecordedat']
                    ]
                    recorded_dates = [d for d in recorded_dates if d is not None]
                    
                    for date in recorded_dates:
                        date_str = date.date().isoformat()
                        
                        if row['followersrecordedat'] and row['followersrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["FollowersCount"] = row['followerscount']
                        
                        if row['engagementrecordedat'] and row['engagementrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["EngagementRate"] = row['engagementrate']
                        
                        if row['rankrecordedat'] and row['rankrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["Position"] = row['position']

                # Convert defaultdict to regular dict and sort by date
                user_data["HistoricalData"] = dict(sorted(user_data["HistoricalData"].items(), reverse=True))
                
                # Remove empty date entries
                user_data["HistoricalData"] = {k: v for k, v in user_data["HistoricalData"].items() if v}
                
                # Use json.dumps with the custom serializer to handle datetime objects
                return {
                    "status_code": 200,
                    "data": json.loads(json.dumps(user_data, default=json_serial))
                }
            else:
                return None
        
    except Exception as e:
        logging.error(f"Failed to query Instagram user data: {e}")
        return {
            "status_code": 500,
            "error": str(e)
        }

async def query_tiktok_user_data(pg_pool, username):
    query_sql = """
    SELECT 
        t.Username, t.ImageURL, r.Position,
        c.CommentsCount, ft.FollowersCount, l.LikesCount, v.ViewsCount, s.SharesCount, 
        c.RecordedAt AS CommentsRecordedAt, ft.RecordedAt AS FollowersRecordedAt, 
        l.RecordedAt AS LikesRecordedAt, v.RecordedAt AS ViewsRecordedAt, s.RecordedAt AS SharesRecordedAt,
        r.RecordedAt AS RankRecordedAt
    FROM tiktok_stats t
    LEFT JOIN comments_history c ON t.ID = c.TikTokStatsID
    LEFT JOIN followers_tiktok ft ON t.ID = ft.TikTokStatsID
    LEFT JOIN likes_history l ON t.ID = l.TikTokStatsID
    LEFT JOIN views_history v ON t.ID = v.TikTokStatsID
    LEFT JOIN shares_history s ON t.ID = s.TikTokStatsID
    LEFT JOIN rank_tiktok r ON t.ID = r.TikTokStatsID
    WHERE t.Username = $1
    ORDER BY ft.RecordedAt DESC, c.RecordedAt DESC, l.RecordedAt DESC, v.RecordedAt DESC, s.RecordedAt DESC, r.RecordedAt DESC;
    """

    try:
        async with pg_pool.acquire() as conn:
            results = await conn.fetch(query_sql, username)
            
            if results:
                user_data = {
                    "Username": results[0]['username'],
                    "ImageURL": results[0]['imageurl'],
                    "HistoricalData": defaultdict(dict)
                }
                
                for row in results:
                    recorded_dates = [
                        row['followersrecordedat'],
                        row['commentsrecordedat'],
                        row['likesrecordedat'],
                        row['viewsrecordedat'],
                        row['sharesrecordedat'],
                        row['rankrecordedat']
                    ]
                    recorded_dates = [d for d in recorded_dates if d is not None]
                    
                    for date in recorded_dates:
                        date_str = date.date().isoformat()
                        
                        if row['followersrecordedat'] and row['followersrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["FollowersCount"] = row['followerscount']
                        
                        if row['commentsrecordedat'] and row['commentsrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["CommentsCount"] = row['commentscount']
                        
                        if row['likesrecordedat'] and row['likesrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["LikesCount"] = row['likescount']
                        
                        if row['viewsrecordedat'] and row['viewsrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["ViewsCount"] = row['viewscount']
                        
                        if row['sharesrecordedat'] and row['sharesrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["SharesCount"] = row['sharescount']
                        
                        if row['rankrecordedat'] and row['rankrecordedat'].date() == date.date():
                            user_data["HistoricalData"][date_str]["Position"] = row['position']

                # Convert defaultdict to regular dict and sort by date
                user_data["HistoricalData"] = dict(sorted(user_data["HistoricalData"].items(), reverse=True))
                
                # Remove empty date entries
                user_data["HistoricalData"] = {k: v for k, v in user_data["HistoricalData"].items() if v}
                
                # Use json.dumps with the custom serializer to handle datetime objects
                return {
                    "status_code": 200,
                    "data": json.loads(json.dumps(user_data, default=json_serial))
                }
            else:
                return None
        
    except Exception as e:
        logging.error(f"Failed to query TikTok user data: {e}")
        return {
            "status_code": 500,
            "error": str(e)
        }


async def update_or_insert_instagram_data_from_csv(pg_pool, csv_file_path):
    upsert_user_sql = """
    INSERT INTO instagram_stats (Username, Category, Country, ImageURL)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (Username) 
    DO UPDATE SET Category = $2, Country = $3, ImageURL = $4;
    """
    insert_rank_sql = """
    INSERT INTO rank_insta (InstagramStatsID, Position, RecordedAt)
    VALUES ((SELECT ID FROM instagram_stats WHERE Username = $1), $2, NOW());
    """
    insert_followers_sql = """
    INSERT INTO followers_insta (InstagramStatsID, FollowersCount, RecordedAt)
    VALUES ((SELECT ID FROM instagram_stats WHERE Username = $1), $2, NOW());
    """
    insert_engagement_sql = """
    INSERT INTO engagement_history (InstagramStatsID, EngagementRate, RecordedAt)
    VALUES ((SELECT ID FROM instagram_stats WHERE Username = $1), $2, NOW());
    """

    df = pd.read_csv(csv_file_path)
    df = df.where(pd.notnull(df), None)

    try:
        async with pg_pool.acquire() as conn:
            async with conn.transaction():
                for index, row in df.iterrows():
                    # Upsert user data
                    await conn.execute(upsert_user_sql, 
                        row['Username'], row['Category'], row['Country'], row['img']
                    )

                    # Always insert new rank, followers, and engagement records
                    await conn.execute(insert_rank_sql, 
                        row['Username'], row['Rank']
                    )
                    await conn.execute(insert_followers_sql, 
                        row['Username'], row['Followers']
                    )
                    await conn.execute(insert_engagement_sql, 
                        row['Username'], row['Engagement']
                    )

                logging.info("Instagram data updated successfully from CSV.")
    except Exception as e:
        logging.error(f"Failed to update or insert Instagram data from CSV: {e}")

async def update_or_insert_tiktok_data_from_csv(pg_pool, csv_file_path):
    upsert_user_sql = """
    INSERT INTO tiktok_stats (Username, ImageURL)
    VALUES ($1, $2)
    ON CONFLICT (Username) 
    DO UPDATE SET ImageURL = $2
    RETURNING ID;
    """

    insert_rank_sql = """
    INSERT INTO rank_tiktok (TikTokStatsID, Position, RecordedAt)
    VALUES ($1, $2, NOW());
    """

    insert_comments_sql = """
    INSERT INTO comments_history (TikTokStatsID, CommentsCount, RecordedAt)
    VALUES ($1, $2, NOW());
    """

    insert_followers_sql = """
    INSERT INTO followers_tiktok (TikTokStatsID, FollowersCount, RecordedAt)
    VALUES ($1, $2, NOW());
    """

    insert_likes_sql = """
    INSERT INTO likes_history (TikTokStatsID, LikesCount, RecordedAt)
    VALUES ($1, $2, NOW());
    """

    insert_views_sql = """
    INSERT INTO views_history (TikTokStatsID, ViewsCount, RecordedAt)
    VALUES ($1, $2, NOW());
    """

    insert_shares_sql = """
    INSERT INTO shares_history (TikTokStatsID, SharesCount, RecordedAt)
    VALUES ($1, $2, NOW());
    """

    df = pd.read_csv(csv_file_path)
    df = df.where(pd.notnull(df), None)

    try:
        async with pg_pool.acquire() as conn:
            async with conn.transaction():
                for index, row in df.iterrows():
                    # Upsert user data and get the user ID
                    user_id = await conn.fetchval(upsert_user_sql, 
                        row['Username'], row['img']
                    )

                    # Always insert new records for all metrics
                    await conn.execute(insert_rank_sql, user_id, row['Rank'])
                    await conn.execute(insert_comments_sql, user_id, row['Comments'])
                    await conn.execute(insert_followers_sql, user_id, row['Followers'])
                    await conn.execute(insert_likes_sql, user_id, row['Likes'])
                    await conn.execute(insert_views_sql, user_id, row['Views'])
                    await conn.execute(insert_shares_sql, user_id, row['Shares'])

            logging.info("TikTok data updated successfully from CSV.")
    except Exception as e:
        logging.error(f"Failed to update or insert TikTok data from CSV: {e}")

async def get_insta_stats(pg_pool):
    query = """
    SELECT 
        insta_stats.ID,
        insta_stats.Username,
        insta_stats.Category,
        insta_stats.Country,
        insta_stats.ImageURL,
        
        -- Today's data
        MAX(f1.FollowersCount) AS FollowersToday,
        MAX(e1.EngagementRate) AS EngagementToday,
        MAX(r1.Position) AS RankToday,
        
        -- 7 days ago
        MAX(f7.FollowersCount) AS Followers7DaysAgo,
        MAX(e7.EngagementRate) AS Engagement7DaysAgo,
        MAX(r7.Position) AS Rank7DaysAgo,
        
        -- 14 days ago
        MAX(f14.FollowersCount) AS Followers14DaysAgo,
        MAX(e14.EngagementRate) AS Engagement14DaysAgo,
        MAX(r14.Position) AS Rank14DaysAgo,
        
        -- 28 days ago
        MAX(f28.FollowersCount) AS Followers28DaysAgo,
        MAX(e28.EngagementRate) AS Engagement28DaysAgo,
        MAX(r28.Position) AS Rank28DaysAgo
    FROM 
        instagram_stats insta_stats
    LEFT JOIN 
        followers_insta f1 ON insta_stats.ID = f1.InstagramStatsID AND DATE(f1.RecordedAt) = CURRENT_DATE
    LEFT JOIN 
        engagement_history e1 ON insta_stats.ID = e1.InstagramStatsID AND DATE(e1.RecordedAt) = CURRENT_DATE
    LEFT JOIN 
        rank_insta r1 ON insta_stats.ID = r1.InstagramStatsID AND DATE(r1.RecordedAt) = CURRENT_DATE
    
    -- 7 days ago joins
    LEFT JOIN 
        followers_insta f7 ON insta_stats.ID = f7.InstagramStatsID AND DATE(f7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    LEFT JOIN 
        engagement_history e7 ON insta_stats.ID = e7.InstagramStatsID AND DATE(e7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    LEFT JOIN 
        rank_insta r7 ON insta_stats.ID = r7.InstagramStatsID AND DATE(r7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    
    -- 14 days ago joins
    LEFT JOIN 
        followers_insta f14 ON insta_stats.ID = f14.InstagramStatsID AND DATE(f14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    LEFT JOIN 
        engagement_history e14 ON insta_stats.ID = e14.InstagramStatsID AND DATE(e14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    LEFT JOIN 
        rank_insta r14 ON insta_stats.ID = r14.InstagramStatsID AND DATE(r14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    
    -- 28 days ago joins
    LEFT JOIN 
        followers_insta f28 ON insta_stats.ID = f28.InstagramStatsID AND DATE(f28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    LEFT JOIN 
        engagement_history e28 ON insta_stats.ID = e28.InstagramStatsID AND DATE(e28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    LEFT JOIN 
        rank_insta r28 ON insta_stats.ID = r28.InstagramStatsID AND DATE(r28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    GROUP BY
        insta_stats.ID, insta_stats.Username, insta_stats.Category, insta_stats.Country, insta_stats.ImageURL
    ORDER BY
        RankToday ASC
    """

    try:
        async with pg_pool.acquire() as conn:
            results = await conn.fetch(query)
            
            # Convert results to a list of dictionaries
            return {
                "status_code": 200,
                "data": [dict(row) for row in results]
            }
    except Exception as e:
        logging.error(f"Failed to retrieve user stats: {e}")
        return {
            "status_code": 500,
            "error": str(e)
        }
    

async def get_tiktok_stats(pg_pool):
    query = """
    SELECT 
        tiktok_stats.ID,
        tiktok_stats.Username,
        tiktok_stats.ImageURL,
        
        -- Today's data
        MAX(f1.FollowersCount) AS FollowersToday,
        MAX(l1.LikesCount) AS LikesToday,
        MAX(v1.ViewsCount) AS ViewsToday,
        MAX(c1.CommentsCount) AS CommentsToday,
        MAX(s1.SharesCount) AS SharesToday,
        MAX(r1.Position) AS RankToday,
        
        -- 7 days ago
        MAX(f7.FollowersCount) AS Followers7DaysAgo,
        MAX(l7.LikesCount) AS Likes7DaysAgo,
        MAX(v7.ViewsCount) AS Views7DaysAgo,
        MAX(c7.CommentsCount) AS Comments7DaysAgo,
        MAX(s7.SharesCount) AS Shares7DaysAgo,
        MAX(r7.Position) AS Rank7DaysAgo,
        
        -- 14 days ago
        MAX(f14.FollowersCount) AS Followers14DaysAgo,
        MAX(l14.LikesCount) AS Likes14DaysAgo,
        MAX(v14.ViewsCount) AS Views14DaysAgo,
        MAX(c14.CommentsCount) AS Comments14DaysAgo,
        MAX(s14.SharesCount) AS Shares14DaysAgo,
        MAX(r14.Position) AS Rank14DaysAgo,
        
        -- 28 days ago
        MAX(f28.FollowersCount) AS Followers28DaysAgo,
        MAX(l28.LikesCount) AS Likes28DaysAgo,
        MAX(v28.ViewsCount) AS Views28DaysAgo,
        MAX(c28.CommentsCount) AS Comments28DaysAgo,
        MAX(s28.SharesCount) AS Shares28DaysAgo,
        MAX(r28.Position) AS Rank28DaysAgo
    FROM 
        tiktok_stats
    LEFT JOIN 
        followers_tiktok f1 ON tiktok_stats.ID = f1.TikTokStatsID AND DATE(f1.RecordedAt) = CURRENT_DATE
    LEFT JOIN 
        likes_history l1 ON tiktok_stats.ID = l1.TikTokStatsID AND DATE(l1.RecordedAt) = CURRENT_DATE
    LEFT JOIN 
        views_history v1 ON tiktok_stats.ID = v1.TikTokStatsID AND DATE(v1.RecordedAt) = CURRENT_DATE
    LEFT JOIN 
        comments_history c1 ON tiktok_stats.ID = c1.TikTokStatsID AND DATE(c1.RecordedAt) = CURRENT_DATE
    LEFT JOIN 
        shares_history s1 ON tiktok_stats.ID = s1.TikTokStatsID AND DATE(s1.RecordedAt) = CURRENT_DATE
    LEFT JOIN 
        rank_tiktok r1 ON tiktok_stats.ID = r1.TikTokStatsID AND DATE(r1.RecordedAt) = CURRENT_DATE
    
    -- 7 days ago joins
    LEFT JOIN 
        followers_tiktok f7 ON tiktok_stats.ID = f7.TikTokStatsID AND DATE(f7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    LEFT JOIN 
        likes_history l7 ON tiktok_stats.ID = l7.TikTokStatsID AND DATE(l7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    LEFT JOIN 
        views_history v7 ON tiktok_stats.ID = v7.TikTokStatsID AND DATE(v7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    LEFT JOIN 
        comments_history c7 ON tiktok_stats.ID = c7.TikTokStatsID AND DATE(c7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    LEFT JOIN 
        shares_history s7 ON tiktok_stats.ID = s7.TikTokStatsID AND DATE(s7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    LEFT JOIN 
        rank_tiktok r7 ON tiktok_stats.ID = r7.TikTokStatsID AND DATE(r7.RecordedAt) = CURRENT_DATE - INTERVAL '7 days'
    
    -- 14 days ago joins
    LEFT JOIN 
        followers_tiktok f14 ON tiktok_stats.ID = f14.TikTokStatsID AND DATE(f14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    LEFT JOIN 
        likes_history l14 ON tiktok_stats.ID = l14.TikTokStatsID AND DATE(l14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    LEFT JOIN 
        views_history v14 ON tiktok_stats.ID = v14.TikTokStatsID AND DATE(v14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    LEFT JOIN 
        comments_history c14 ON tiktok_stats.ID = c14.TikTokStatsID AND DATE(c14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    LEFT JOIN 
        shares_history s14 ON tiktok_stats.ID = s14.TikTokStatsID AND DATE(s14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    LEFT JOIN 
        rank_tiktok r14 ON tiktok_stats.ID = r14.TikTokStatsID AND DATE(r14.RecordedAt) = CURRENT_DATE - INTERVAL '14 days'
    
    -- 28 days ago joins
    LEFT JOIN 
        followers_tiktok f28 ON tiktok_stats.ID = f28.TikTokStatsID AND DATE(f28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    LEFT JOIN 
        likes_history l28 ON tiktok_stats.ID = l28.TikTokStatsID AND DATE(l28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    LEFT JOIN 
        views_history v28 ON tiktok_stats.ID = v28.TikTokStatsID AND DATE(v28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    LEFT JOIN 
        comments_history c28 ON tiktok_stats.ID = c28.TikTokStatsID AND DATE(c28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    LEFT JOIN 
        shares_history s28 ON tiktok_stats.ID = s28.TikTokStatsID AND DATE(s28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    LEFT JOIN 
        rank_tiktok r28 ON tiktok_stats.ID = r28.TikTokStatsID AND DATE(r28.RecordedAt) = CURRENT_DATE - INTERVAL '28 days'
    GROUP BY
        tiktok_stats.ID, tiktok_stats.Username, tiktok_stats.ImageURL
    ORDER BY
        RankToday ASC
    """

    try:
        async with pg_pool.acquire() as conn:
            results = await conn.fetch(query)
            
            # Convert results to a list of dictionaries
            return {
                "status_code": 200,
                "data": [dict(row) for row in results]
            }
    except Exception as e:
        logging.error(f"Failed to retrieve TikTok stats: {e}")
        return {
            "status_code": 500,
            "error": str(e)
        }

async def delete_old_insta_data(mysql_pool):
    one_month_ago = datetime.utcnow() - timedelta(days=30)

    delete_queries = [
        "followers_insta",
        "engagement_history",
        "rank_insta"
    ]

    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                for table in delete_queries:
                    # Delete old data
                    delete_sql = f"""
                    DELETE FROM {table}
                    WHERE RecordedAt < %s;
                    """
                    await cursor.execute(delete_sql, (one_month_ago,))
                    deleted_count = cursor.rowcount
                    logging.info(f"Deleted {deleted_count} old records from {table}")

                    # Reset the auto-increment
                    reset_auto_increment_sql = f"""
                    ALTER TABLE {table} AUTO_INCREMENT = 1;
                    """
                    await cursor.execute(reset_auto_increment_sql)
                    logging.info(f"Reset auto-increment for {table}")

                await conn.commit()

        logging.info("Old Instagram data deletion and auto-increment reset completed successfully.")
    except Exception as e:
        logging.error(f"Failed to delete old Instagram data or reset auto-increments: {e}")

async def delete_old_tiktok_data(pg_pool):
    one_month_ago = datetime.utcnow() - timedelta(days=30)

    delete_queries = [
        ("comments_history", "CommentsCount"),
        ("followers_tiktok", "FollowersCount"),
        ("likes_history", "LikesCount"),
        ("views_history", "ViewsCount"),
        ("shares_history", "SharesCount"),
        ("rank_tiktok", "Position")
    ]

    try:
        async with pg_pool.acquire() as conn:
            async with conn.transaction():
                for table, count_column in delete_queries:
                    # Delete old data
                    delete_sql = f"""
                    DELETE FROM {table}
                    WHERE RecordedAt < $1;
                    """
                    result = await conn.execute(delete_sql, one_month_ago)
                    deleted_count = int(result.split()[-1])
                    logging.info(f"Deleted {deleted_count} old records from {table}")

                    # Reset the sequence
                    reset_sequence_sql = f"""
                    SELECT setval(pg_get_serial_sequence('{table}', 'id'), 
                                  COALESCE((SELECT MAX(id) FROM {table}), 0) + 1, 
                                  false);
                    """
                    await conn.execute(reset_sequence_sql)
                    logging.info(f"Reset sequence for {table}")

        logging.info("Old TikTok data deletion and sequence reset completed successfully.")
    except Exception as e:
        logging.error(f"Failed to delete old TikTok data or reset sequences: {e}")