import requests
from bs4 import BeautifulSoup
import csv
import os
import json
import pandas as pd
import numpy as np

async def tiktok_scrap():
    try:
        csv_file_name = 'scraped_data_tiktok.csv'
        file_exists = os.path.isfile(csv_file_name)
        if file_exists:
            os.remove(csv_file_name)
        scraped = False

        for i in range(1, 20):
            url = f'https://hypeauditor.com/top-tiktok-singapore/?p={i}'
            response = requests.get(url)
            if response.status_code == 200:
                html_content = response.text
                soup = BeautifulSoup(html_content, 'html.parser')
                data = []
                rows = soup.find_all('div', class_='row tiktok-row')
                for row in rows:
                    rank = row.find(class_='row-cell rank').get_text(strip=True) if row.find(class_='row-cell rank') else ''
                    username = row.find(class_='contributor__name-content').get_text(strip=True) if row.find(class_='contributor__name-content') else ''
                    comments = row.find(class_='row-cell comments-avg').get_text(strip=True) if row.find(class_='row-cell comments-avg') else ''
                    followers = row.find(class_='row-cell subscribers').get_text(strip=True) if row.find(class_='row-cell subscribers') else ''
                    views = row.find(class_='row-cell views-avg').get_text(strip=True) if row.find(class_='row-cell views-avg') else ''
                    likes = row.find(class_='row-cell likes-avg').get_text(strip=True) if row.find(class_='row-cell likes-avg') else ''
                    shares = row.find(class_='row-cell shares-avg').get_text(strip=True) if row.find(class_='row-cell shares-avg') else ''
                    img = row.find('img')['src'] if row.find('img') else ''
                    data.append([rank, username, comments, followers, likes, views, shares,img])
                
                with open(csv_file_name, 'a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if not file_exists:
                        writer.writerow(['Rank', 'Username', 'Comments', 'Followers', 'Likes', 'Views', 'Shares', 'img'])
                        file_exists = True 
                    writer.writerows(data)
                scraped = True
            else:
                scraped = False

        file_name = 'scraped_data_tiktok.csv'
        temp_file_name = file_name + '.tmp'

        with open(file_name, mode='r', newline='', encoding='utf-8') as infile, \
            open(temp_file_name, mode='w', newline='', encoding='utf-8') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            headers = ['Rank', 'Username', 'Comments', 'Followers', 'Likes', 'Views', 'Shares', 'img']
            writer.writerow(headers)
            
            for row in reader:
                if any(field.strip() for field in row):
                    writer.writerow(row)

        os.remove(file_name)
        os.rename(temp_file_name, file_name)
        if scraped:
            return {
                "status_code": 200,
                "message": "Data scraped successfully"
            }
        else:
            return {
                "status_code": 500,
                "error": "Failed to scrape data"
            }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }


async def instagram_scrap():
    try:
        csv_file_name = 'scraped_data_instagram.csv'
        file_exists = os.path.isfile(csv_file_name)
        if file_exists:
            os.remove(csv_file_name)
        scraped = False

        for i in range(1, 20):
            url = f'https://hypeauditor.com/top-instagram-all-singapore/?p={i}'
            response = requests.get(url)
            if response.status_code == 200:
                html_content = response.text
                soup = BeautifulSoup(html_content, 'html.parser')
                data = []
                rows = soup.find_all('div', class_='row')
                for row in rows:
                    rank = row.find(class_='row-cell rank').get_text(strip=True) if row.find(class_='row-cell rank') else ''
                    username = row.find(class_='contributor__name-content').get_text(strip=True) if row.find(class_='contributor__name-content') else ''
                    category = row.find(class_='tag__content ellipsis').get_text(strip=True) if row.find(class_='tag__content ellipsis') else ''
                    followers = row.find(class_='row-cell subscribers').get_text(strip=True) if row.find(class_='row-cell subscribers') else ''
                    country = row.find(class_='row-cell audience').get_text(strip=True) if row.find(class_='row-cell audience') else ''
                    engagement = row.find(class_='row-cell engagement').get_text(strip=True) if row.find(class_='row-cell engagement') else ''
                    img = row.find('img')['src'] if row.find('img') else ''
                    data.append([rank, username, category, followers, country, engagement, img])
                
                with open(csv_file_name, 'a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if not file_exists:
                        writer.writerow(['Rank', 'Username', 'Category', 'Followers', 'Country', 'Engagement', 'img'])
                        file_exists = True  
                    writer.writerows(data)
                scraped = True
            else:
                scraped = False

        file_name = 'scraped_data_instagram.csv'
        temp_file_name = file_name + '.tmp'

        with open(file_name, mode='r', newline='', encoding='utf-8') as infile, \
            open(temp_file_name, mode='w', newline='', encoding='utf-8') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            headers = ['Rank', 'Username', 'Category', 'Followers', 'Country', 'Engagement', 'img']
            writer.writerow(headers)
            
            for row in reader:
                if any(field.strip() for field in row):
                    writer.writerow(row)

        os.remove(file_name)
        os.rename(temp_file_name, file_name)
        if scraped:
            return {
                "status_code": 200,
                "message": "Data scraped successfully"
            }
        else:
            return {
                "status_code": 500,
                "error": "Failed to scrape data"
            }
    except Exception as e:
        return {
            "status_code": 500,
            "error": str(e)
        }
            
async def csv_to_json(filename):
    df = pd.read_csv(filename)
    
    # Replace non-compliant float values
    df.replace([np.inf, -np.inf], np.nan, inplace=True)  # Replace Inf/-Inf with NaN
    df.fillna('null', inplace=True)  # Replace NaN with a JSON compliant value
    
    json_data = {}
    for index, row in df.iterrows():
        row_dict = row.to_dict()
        # Convert non-compliant float values in row_dict if needed
        for key, value in row_dict.items():
            if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                row_dict[key] = 'null'  # or any other compliant value you prefer
        json_data[row_dict['Rank']] = row_dict
    
    return {
        "status_code": 200,
        "data": json_data
    }

