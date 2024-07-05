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

async def serpapi(media = None, q = None, topic = None, story = None):
    non_none_params = sum(param is not None for param in [media, q, topic, story])
    if non_none_params >1:
        return {
            "status_code": 400,
            "error": "Please provide exactly one parameter."
        }
    
    try:
        url = "https://serpapi.com/search"
        params = {
            'engine': 'google_news',
            'gl': 'sg',
            'api_key': os.getenv("SERPAPI_KEY"),
        }

        if media is not None:
            if media.lower() == 'cnn':
                params['publication_token'] = os.getenv("CNN")
            elif media.lower() == 'bbc':
                params['publication_token'] = os.getenv("BBC")
            elif media.lower() == 'guardian':
                params['publication_token'] = os.getenv("THE_GUARDIAN")
            else:
                return {
                    "status_code": 400,
                    "error": "Invalid media"
                }
        if q:
            params['q'] = q
        if topic is not None:
            if topic.lower() == 'sports':
                params['topic_token'] = os.getenv("SPORTS")
            elif topic.lower() == 'business':
                params['topic_token'] = os.getenv("BUSINESS")
            elif topic.lower() == 'technology':
                params['topic_token'] = os.getenv("TECHNOLOGY")
            elif topic.lower() == 'entertainment':
                params['topic_token'] = os.getenv("ENTERTAINMENT")
            elif topic.lower() == 'health':
                params['topic_token'] = os.getenv("HEALTH")
            elif topic.lower() == 'science':
                params['topic_token'] = os.getenv("SCIENCE")
            else:
                return {
                    "status_code": 400,
                    "error": "Invalid topic"
                }
        if story:
            params['story_token'] = story

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
    
async def news_username():
    try:
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
        return {
            "status_code": 200,
            "data": news_list
        }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }