import requests
import os
from dotenv import load_dotenv

load_dotenv()

async def get_trend_1():
    try :
        url = "https://twitter-trends-by-location.p.rapidapi.com/location/06cdfdb4c3a53e54a644bfa087a46a37"
        headers = {
            "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
            "x-rapidapi-host": "twitter-trends-by-location.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers)
        return {
            "status_code": response.status_code,
            "data": response.json()
        }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }
    