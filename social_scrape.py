import requests
from bs4 import BeautifulSoup
import csv
import os

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
                    data.append([rank, username, comments, followers, likes, views, shares])
                
                with open(csv_file_name, 'a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if not file_exists:
                        writer.writerow(['Rank', 'Username', 'Comments', 'Followers', 'Likes', 'Views', 'Shares'])
                        file_exists = True 
                    writer.writerows(data)
                scraped = True
            else:
                scraped = False

        file_name = 'scraped_data_tiktok.csv'
        temp_file_name = file_name + '.tmp'

        # Step 1: Read the file and filter out unwanted rows
        with open(file_name, mode='r', newline='', encoding='utf-8') as infile, \
            open(temp_file_name, mode='w', newline='', encoding='utf-8') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
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
