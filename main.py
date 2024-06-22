from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from hashtag import hashtag
from business_discovery import business_discovery, fetch_business_discovery
app = FastAPI()

# CORS
origins = [
    "http://localhost",
    "http://localhost:8080",
]

@app.get("/hashtag/top_media")
async def get_hashtag(q: str):
    return await hashtag(q,top_media=True)

@app.get("/hashtag/recent_media")
async def get_hashtag(q: str):
    return await hashtag(q,top_media=False)

@app.get("/getuser")
async def get_business_discovery(username: str):
    return await business_discovery(username)

@app.get("/getbulkuser")
async def get_bulk_business_discovery(usernames: str):
    return await fetch_business_discovery(usernames)