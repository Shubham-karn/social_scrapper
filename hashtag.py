import requests
import os
from dotenv import load_dotenv

load_dotenv()

async def hashtag(q,top_media):
    try:
        url = f"https://graph.facebook.com/v20.0/ig_hashtag_search"

        params = {
            "access_token": os.getenv("ACCESS_TOKEN"),
            "user_id": os.getenv("USER_ID"),
            "q": q
        }

        response = requests.get(url, params=params)

        has_id = response.json()

        # Extract the ID from the response and save it in a variable
        hashtag_id = has_id['data'][0]['id']

        if top_media == False:
            url = f"https://graph.facebook.com/v20.0/{hashtag_id}/recent_media"
        else:
            url = f"https://graph.facebook.com/v20.0/{hashtag_id}/top_media"

        params = {
            "access_token": os.getenv("ACCESS_TOKEN"),
            "user_id": os.getenv("USER_ID"),
            "fields" : "permalink,like_count,media_url"
        }

        response = requests.get(url, params=params)

        return {
            "status_code": response.status_code,
            "data": response.json()
        }
    
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }
