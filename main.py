from fastapi import FastAPI
import json
import logging
from typing import Optional
from pydantic import BaseModel
from hashtag import hashtag
from business_discovery import business_discovery, fetch_business_discovery
from social_scrape import tiktok_scrap, instagram_scrap
from news import get_instagram_news, newsapi, news_data, serpapi, news_username
from summarizer import summary
from location import get_city, get_location, get_city_url
from trend import get_trend_1
from database import get_redis, get_mysql_pool, create_insta_table, create_tiktok_tables
from query_data import update_or_insert_instagram_data_from_csv, update_or_insert_tiktok_data_from_csv, get_insta_stats, get_tiktok_stats
from query_data import query_insta_user_data, query_tiktok_user_data
from apscheduler.schedulers.asyncio import AsyncIOScheduler

app = FastAPI()
scheduler = AsyncIOScheduler()

redis = None
mysql_pool = None

@app.on_event("startup")
async def startup_event():
    global redis
    redis = await get_redis()
    mysql_pool = await get_mysql_pool() 
    await create_insta_table(mysql_pool)
    await create_tiktok_tables(mysql_pool)

@app.on_event("shutdown")
async def shutdown_event():
    global redis, mysql_pool
    if redis:
        redis.close()
        await redis.wait_closed()
        logging.info("Redis connection closed.")
    if mysql_pool:
        mysql_pool.close()
        await mysql_pool.wait_closed()
        logging.info("MySQL connection pool closed.")

class Article(BaseModel):
    article: str

# CORS
origins = [
    "http://localhost",
    "http://localhost:8080",
]

@app.get("/check")
async def root():
    try:
        return {
            "status": 200,
            "message": "Server is up and running"
            }
    except Exception as e:
        return {
            "status": 500,
            "error": str(e)
            }

@app.get("/hashtag/top_media")
async def get_hashtag(q: str):
    cache_key = f"hashtag-top_media-{q}"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data = await hashtag(q,top_media=True)
    await redis.set(cache_key, json.dumps(data), expire=600)
    return data

@app.get("/hashtag/recent_media")
async def get_hashtag(q: str):
    cache_key = f"hashtag-recent_media-{q}"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data = await hashtag(q,top_media=False)
    await redis.set(cache_key, json.dumps(data), expire=600)
    return data

@app.get("/getuser")
async def get_business_discovery(username: str):
    cache_key = f"business-discovery-{username}"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data =  await business_discovery(username)
    await redis.set(cache_key, json.dumps(data), expire=600)
    return data

@app.get("/getbulkuser")
async def get_bulk_business_discovery(usernames: str):
    cache_key = f"business-discovery-bulk-{usernames}"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data =  await fetch_business_discovery(usernames)
    await redis.set(cache_key, json.dumps(data), expire=600)
    return data

@app.get("/scrape_tiktok")
async def scrape_tiktok():
    # cache_key = "scrape-tiktok"
    # cached_data = await redis.get(cache_key)
    # if cached_data:
    #     return json.loads(cached_data)
    
    mysql_pool = await get_mysql_pool()
    # data = await tiktok_scrap()
    await update_or_insert_tiktok_data_from_csv(mysql_pool, 'scraped_data_tiktok.csv')
    # await redis.set(cache_key, json.dumps(data), expire=86400)
    return {
        "status_code": 200,
    }

@app.get("/scrape_instagram")
async def scrape_instagram():
    cache_key = "scrape-instagram"
    # cached_data = await redis.get(cache_key)
    # if cached_data:
    #     return json.loads(cached_data)
    
    mysql_pool = await get_mysql_pool()
    # data = await instagram_scrap()
    await update_or_insert_instagram_data_from_csv(mysql_pool, 'scraped_data_instagram.csv')
    # await redis.set(cache_key, json.dumps(data), expire=86400)
    return {
        "status_code": 200,
    }

@app.get("/tiktok/influencers")
async def get_tiktok_influencers():
    mysql_pool = await get_mysql_pool()
    return await get_tiktok_stats(mysql_pool)

@app.get("/instagram/influencers")
async def get_instagram_influencers():
    mysql_pool = await get_mysql_pool()
    return await get_insta_stats(mysql_pool)

@app.get("/instagram/{username}")
async def get_instagram_data(username: str):
    mysql_pool = await get_mysql_pool()
    data = await query_insta_user_data(mysql_pool, username)
    return data

@app.get("/tiktok/{username}")
async def get_tiktok_data(username: str):
    mysql_pool = await get_mysql_pool()
    data = await query_tiktok_user_data(mysql_pool, username)
    return data

@app.get("/instagram/news")
async def get_news():
    cache_key = "instagram-news"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data = await get_instagram_news()
    await redis.set(cache_key, json.dumps(data), expire=600)
    return data

@app.get("/newsapi")
async def get_news():
    cache_key = "newsapi"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data =  await newsapi()
    await redis.set(cache_key, json.dumps(data), expire=3600)
    return data

@app.get("/newsdata")
async def get_news():
    cache_key = "newsdata"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    data =  await news_data()
    await redis.set(cache_key, json.dumps(data), expire=435)
    return data

@app.get("/news")
async def get_news(media: Optional[str] = None, q: Optional[str] = None, topic: Optional[str] = None, story: Optional[str] = None):
    cache_key = f"news-{media}-{q}-{topic}-{story}"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data = await serpapi(media, q, topic, story)
    await redis.set(cache_key, json.dumps(data), expire=25920)
    return data

@app.get("/newspages")
async def get_news_username():
    return await news_username()

@app.post("/summary")
async def get_summary(article_data: Article):
    article = article_data.article
    return await summary(article)

@app.get("/city")
async def get_city_data(q: Optional[str] = None):
    if q:
        return await get_city_url(q)
    return await get_city()

@app.get("/location_post")
async def get_location_data(city: str, place: str):
    return await get_location(city, place)

@app.get("/trend")
async def get_trend():
    cache_key = "trend1"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data = await get_trend_1()
    await redis.set(cache_key, json.dumps(data), expire=25920)
    return data

scheduler.add_job(scrape_instagram, 'cron', hour=1, minute=0)

scheduler.add_job(scrape_tiktok, 'cron', hour=2, minute=10)

@app.on_event("startup")
async def start_scheduler():
    scheduler.start()

