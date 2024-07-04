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
    
async def get_trend_2():
    try:
        url = "https://twitter-trends5.p.rapidapi.com/twitter/request.php"

        payload = "-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"woeid\"\r\n\r\n23424948\r\n-----011000010111000001101001--\r\n\r\n"
        headers = {
            "x-rapidapi-key": "feff2d45a3msh21f16d6a7d0d4eap1cd43djsn7d8cb4bcbf33",
            "x-rapidapi-host": "twitter-trends5.p.rapidapi.com",
            "Content-Type": "multipart/form-data; boundary=---011000010111000001101001"
        }

        response = requests.post(url, data=payload, headers=headers,)
        return {
            "status_code": response.status_code,
            "data": response.json()
        }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }
