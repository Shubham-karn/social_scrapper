import logging
import pandas as pd
import aiomysql
import json
from datetime import datetime, timedelta
from collections import defaultdict

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

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
        ORDER BY f.RecordedAt DESC, e.RecordedAt DESC, r.RecordedAt DESC
    """
    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query_sql, (username,))
                results = await cur.fetchall()
                
                if results:
                    user_data = {
                        "Username": results[0]['Username'],
                        "Category": results[0]['Category'],
                        "Country": results[0]['Country'],
                        "ImageURL": results[0]['ImageURL'],
                        "HistoricalData": defaultdict(dict)
                    }
                    
                    for row in results:
                        date_str = row['FollowersRecordedAt'].date().isoformat()
                        if date_str not in user_data["HistoricalData"]:
                            user_data["HistoricalData"][date_str] = {
                                "FollowersCount": None,
                                "EngagementRate": None,
                                "Position": None
                            }
                        
                        if row['FollowersCount'] is not None:
                            user_data["HistoricalData"][date_str]["FollowersCount"] = row['FollowersCount']
                        
                        if row['EngagementRate'] is not None:
                            user_data["HistoricalData"][date_str]["EngagementRate"] = row['EngagementRate']
                        
                        if row['Position'] is not None:
                            user_data["HistoricalData"][date_str]["Position"] = row['Position']
                    
                    # Convert defaultdict to regular dict and sort by date
                    user_data["HistoricalData"] = dict(sorted(user_data["HistoricalData"].items(), reverse=True))
                    
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

async def query_tiktok_user_data(mysql_pool, username):
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
    WHERE t.Username = %s
    ORDER BY ft.RecordedAt DESC, c.RecordedAt DESC, l.RecordedAt DESC, v.RecordedAt DESC, s.RecordedAt DESC, r.RecordedAt DESC;
    """
    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query_sql, (username,))
                results = await cur.fetchall()
                
                if results:
                    user_data = {
                        "Username": results[0]['Username'],
                        "ImageURL": results[0]['ImageURL'],
                        "HistoricalData": defaultdict(dict)
                    }
                    
                    for row in results:
                        date_str = row['FollowersRecordedAt'].date().isoformat()
                        if date_str not in user_data["HistoricalData"]:
                            user_data["HistoricalData"][date_str] = {
                                "Position": None,
                                "CommentsCount": None,
                                "FollowersCount": None,
                                "LikesCount": None,
                                "ViewsCount": None,
                                "SharesCount": None
                            }
                        
                        if row['Position'] is not None:
                            user_data["HistoricalData"][date_str]["Position"] = row['Position']
                        if row['CommentsCount'] is not None:
                            user_data["HistoricalData"][date_str]["CommentsCount"] = row['CommentsCount']
                        if row['FollowersCount'] is not None:
                            user_data["HistoricalData"][date_str]["FollowersCount"] = row['FollowersCount']
                        if row['LikesCount'] is not None:
                            user_data["HistoricalData"][date_str]["LikesCount"] = row['LikesCount']
                        if row['ViewsCount'] is not None:
                            user_data["HistoricalData"][date_str]["ViewsCount"] = row['ViewsCount']
                        if row['SharesCount'] is not None:
                            user_data["HistoricalData"][date_str]["SharesCount"] = row['SharesCount']
                    
                    # Convert defaultdict to regular dict and sort by date
                    user_data["HistoricalData"] = dict(sorted(user_data["HistoricalData"].items(), reverse=True))
                    
                    # Use json.dumps with the custom serializer to handle datetime objects
                    return {
                        "status_code": 200,
                        "data": json.loads(json.dumps(user_data, default=json_serial))
                    }
                else:
                    return None
            
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

async def get_insta_stats(mysql_pool):
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
        followers_insta f1 ON insta_stats.ID = f1.InstagramStatsID AND DATE(f1.RecordedAt) = CURDATE()
    LEFT JOIN 
        engagement_history e1 ON insta_stats.ID = e1.InstagramStatsID AND DATE(e1.RecordedAt) = CURDATE()
    LEFT JOIN 
        rank_insta r1 ON insta_stats.ID = r1.InstagramStatsID AND DATE(r1.RecordedAt) = CURDATE()
    
    -- 7 days ago joins
    LEFT JOIN 
        followers_insta f7 ON insta_stats.ID = f7.InstagramStatsID AND DATE(f7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    LEFT JOIN 
        engagement_history e7 ON insta_stats.ID = e7.InstagramStatsID AND DATE(e7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    LEFT JOIN 
        rank_insta r7 ON insta_stats.ID = r7.InstagramStatsID AND DATE(r7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    
    -- 14 days ago joins
    LEFT JOIN 
        followers_insta f14 ON insta_stats.ID = f14.InstagramStatsID AND DATE(f14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    LEFT JOIN 
        engagement_history e14 ON insta_stats.ID = e14.InstagramStatsID AND DATE(e14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    LEFT JOIN 
        rank_insta r14 ON insta_stats.ID = r14.InstagramStatsID AND DATE(r14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    
    -- 28 days ago joins
    LEFT JOIN 
        followers_insta f28 ON insta_stats.ID = f28.InstagramStatsID AND DATE(f28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    LEFT JOIN 
        engagement_history e28 ON insta_stats.ID = e28.InstagramStatsID AND DATE(e28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    LEFT JOIN 
        rank_insta r28 ON insta_stats.ID = r28.InstagramStatsID AND DATE(r28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    GROUP BY
        insta_stats.ID, insta_stats.Username, insta_stats.Category, insta_stats.Country, insta_stats.ImageURL
    ORDER BY
        RankToday ASC
    """

    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                results = await cur.fetchall()
                
                # Convert results to a list of dictionaries
                columns = [column[0] for column in cur.description]
                return {
                    "status_code": 200,
                    "data": [dict(zip(columns, row)) for row in results]
                }
    except Exception as e:
        logging.error(f"Failed to retrieve user stats: {e}")
        return {
            "status_code": 500,
            "error": str(e)
        }
    

async def get_tiktok_stats(mysql_pool):
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
        followers_tiktok f1 ON tiktok_stats.ID = f1.TikTokStatsID AND DATE(f1.RecordedAt) = CURDATE()
    LEFT JOIN 
        likes_history l1 ON tiktok_stats.ID = l1.TikTokStatsID AND DATE(l1.RecordedAt) = CURDATE()
    LEFT JOIN 
        views_history v1 ON tiktok_stats.ID = v1.TikTokStatsID AND DATE(v1.RecordedAt) = CURDATE()
    LEFT JOIN 
        comments_history c1 ON tiktok_stats.ID = c1.TikTokStatsID AND DATE(c1.RecordedAt) = CURDATE()
    LEFT JOIN 
        shares_history s1 ON tiktok_stats.ID = s1.TikTokStatsID AND DATE(s1.RecordedAt) = CURDATE()
    LEFT JOIN 
        rank_tiktok r1 ON tiktok_stats.ID = r1.TikTokStatsID AND DATE(r1.RecordedAt) = CURDATE()
    
    -- 7 days ago joins
    LEFT JOIN 
        followers_tiktok f7 ON tiktok_stats.ID = f7.TikTokStatsID AND DATE(f7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    LEFT JOIN 
        likes_history l7 ON tiktok_stats.ID = l7.TikTokStatsID AND DATE(l7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    LEFT JOIN 
        views_history v7 ON tiktok_stats.ID = v7.TikTokStatsID AND DATE(v7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    LEFT JOIN 
        comments_history c7 ON tiktok_stats.ID = c7.TikTokStatsID AND DATE(c7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    LEFT JOIN 
        shares_history s7 ON tiktok_stats.ID = s7.TikTokStatsID AND DATE(s7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    LEFT JOIN 
        rank_tiktok r7 ON tiktok_stats.ID = r7.TikTokStatsID AND DATE(r7.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    
    -- 14 days ago joins
    LEFT JOIN 
        followers_tiktok f14 ON tiktok_stats.ID = f14.TikTokStatsID AND DATE(f14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    LEFT JOIN 
        likes_history l14 ON tiktok_stats.ID = l14.TikTokStatsID AND DATE(l14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    LEFT JOIN 
        views_history v14 ON tiktok_stats.ID = v14.TikTokStatsID AND DATE(v14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    LEFT JOIN 
        comments_history c14 ON tiktok_stats.ID = c14.TikTokStatsID AND DATE(c14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    LEFT JOIN 
        shares_history s14 ON tiktok_stats.ID = s14.TikTokStatsID AND DATE(s14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    LEFT JOIN 
        rank_tiktok r14 ON tiktok_stats.ID = r14.TikTokStatsID AND DATE(r14.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 14 DAY)
    
    -- 28 days ago joins
    LEFT JOIN 
        followers_tiktok f28 ON tiktok_stats.ID = f28.TikTokStatsID AND DATE(f28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    LEFT JOIN 
        likes_history l28 ON tiktok_stats.ID = l28.TikTokStatsID AND DATE(l28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    LEFT JOIN 
        views_history v28 ON tiktok_stats.ID = v28.TikTokStatsID AND DATE(v28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    LEFT JOIN 
        comments_history c28 ON tiktok_stats.ID = c28.TikTokStatsID AND DATE(c28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    LEFT JOIN 
        shares_history s28 ON tiktok_stats.ID = s28.TikTokStatsID AND DATE(s28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    LEFT JOIN 
        rank_tiktok r28 ON tiktok_stats.ID = r28.TikTokStatsID AND DATE(r28.RecordedAt) = DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    GROUP BY
        tiktok_stats.ID, tiktok_stats.Username, tiktok_stats.ImageURL
    ORDER BY
        RankToday ASC
    """

    try:
        async with mysql_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                results = await cur.fetchall()
                
                # Convert results to a list of dictionaries
                columns = [column[0] for column in cur.description]
                return {
                    "status_code": 200,
                    "data": [dict(zip(columns, row)) for row in results]
                }
    except Exception as e:
        logging.error(f"Failed to retrieve TikTok stats: {e}")
        return {
            "status_code": 500,
            "error": str(e)
        }