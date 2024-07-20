import requests
import os
from dotenv import load_dotenv

load_dotenv()

async def business_discovery(username):
    try:
        user_id = os.getenv("USER_ID")
        url = f"https://graph.facebook.com/v3.2/{user_id}"

        params = {
            "fields": f"business_discovery.username({username}){{website,profile_picture_url,follows_count,followers_count,media_count,media{{permalink,like_count,media_url,media_type,caption}}}}",
            "access_token": os.getenv("ACCESS_TOKEN"),
        }

        # Send the GET request
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
    
async def fetch_business_discovery(usernames: str):
    try:
        user_id = os.getenv("USER_ID")
        url = f"https://graph.facebook.com/v3.2/{user_id}"

        all_responses = {}

        news_list = usernames.split(',')

        for news in news_list:
            params = {
                "fields": f"business_discovery.username({news}){{followers_count,media_count,media{{permalink,like_count,media_url,media_type,caption}}}}",
                "access_token": os.getenv("ACCESS_TOKEN"),
            }

            # Send the GET request
            response = requests.get(url, params=params)

            if response.status_code == 200:
                all_responses[news] = response.json()
            else:
                all_responses[news] = {"error": response.json()}

        return {
            "status_code": response.status_code,
            "data": all_responses
        }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }
