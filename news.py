import requests
import os
from dotenv import load_dotenv
from newsdataapi import NewsDataApiClient

load_dotenv()

    
async def get_instagram_news():
    try:
        user_id = os.getenv("USER_ID")
        url = f"https://graph.facebook.com/v3.2/{user_id}"

        all_responses = {}

        news_list = ["mothershipsg",
            "channelnewsasia",
            "mustsharenews",
            "8worldnews",
            "sgnewsdaily",
            "fastnews.sg",
            "sgfollowsall",
            "sphmediasg",
            "alvinshtan",
            "zaobaosg",
            "lianhewanbao",
            "shinmindailynews",
            "stompsingapore",
            "uweeklysg",
            "thenewpaper"]

        for news in news_list:
            params = {
                "fields" : f"business_discovery.username({news}){{followers_count,media_count,media{{permalink,like_count,media_url,media_type,caption}}}}",
                "access_token": os.getenv("ACCESS_TOKEN"),
            }

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

async def newsapi():
    try:
        url = "https://newsapi.org/v2/top-headlines"
        params = {
            'country': 'sg',
            'apiKey': os.getenv("NEWS_API_KEY")
        }

        response = requests.get(url, params=params)

        data = response.json()

        return {
            "status_code": response.status_code,
            "data": data
        }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }
    
async def news_data():
    try:
        api = NewsDataApiClient(apikey=os.getenv("NEWS_DATA_KEY"))
        response = api.news_api(country = "sg")
        return {
            "status_code": 200,
            "data": response
            }
    except Exception as e:
        return {
        "status_code": 500,
        "error": str(e)
        }