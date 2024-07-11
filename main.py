from fastapi import FastAPI
import aioredis
import aiomysql
import json
from typing import Optional
from pydantic import BaseModel
from hashtag import hashtag
from business_discovery import business_discovery, fetch_business_discovery
from social_scrape import tiktok_scrap, instagram_scrap, csv_to_json
from news import get_instagram_news, newsapi, news_data, serpapi, news_username
from summarizer import summary
from location import get_city, get_location, get_city_url
from trend import get_trend_1
from apscheduler.schedulers.asyncio import AsyncIOScheduler

app = FastAPI()
scheduler = AsyncIOScheduler()

redis = None
mysql_pool = None

async def get_redis():
    return await aioredis.create_redis_pool(
        'redis://redis_container:6379', encoding='utf8'
    )

async def get_mysql_pool():
    global mysql_pool
    if mysql_pool is None:
        mysql_pool = await aiomysql.create_pool(
            host='mysql_container',  # Docker service name for MySQL
            port=3306,  # Default MySQL port
            user='your_mysql_user',
            password='your_mysql_password',
            db='your_database_name',
            charset='utf8',
            autocommit=True,
        )
    return mysql_pool

@app.on_event("startup")
async def startup_event():
    global redis
    redis = await get_redis()
    await get_mysql_pool()  # Initialize MySQL pool on startup

@app.on_event("shutdown")
async def shutdown_event():
    if redis:
        redis.close()
        await redis.wait_closed()
    if mysql_pool:
        mysql_pool.close()
        await mysql_pool.wait_closed()

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
    cache_key = "scrape-tiktok"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data = await tiktok_scrap()
    await redis.set(cache_key, json.dumps(data), expire=86400)
    return data

@app.get("/scrape_instagram")
async def scrape_instagram():
    cache_key = "scrape-instagram"
    cached_data = await redis.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    
    data = await instagram_scrap()
    await redis.set(cache_key, json.dumps(data), expire=86400)
    return data

@app.get("/tiktok/influencers")
async def get_tiktok_influencers():
    return await csv_to_json('scraped_data_tiktok.csv')

@app.get("/instagram/influencers")
async def get_instagram_influencers():
    return await csv_to_json('scraped_data_instagram.csv')

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

scheduler.add_job(scrape_instagram, 'cron', hour=0, minute=0)

scheduler.add_job(scrape_tiktok, 'cron', hour=0, minute=10)

@app.on_event("startup")
async def start_scheduler():
    scheduler.start()