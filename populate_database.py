import pandas as pd
import mysql.connector

# Load the CSV file
df = pd.read_csv('/scraped_data_instagram.csv')

# Connect to MySQL
cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='summarizer')
cursor = cnx.cursor()

# Populate instagram_stats table
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO instagram_stats (Username, Category, Country, ImageURL, `Rank`)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Username=VALUES(Username)
    """, (row['Username'], row['Category'], row['Country'], row['img'], row['Rank']))
    cnx.commit()

# Populate followers_history and engagement_history tables
for index, row in df.iterrows():
    cursor.execute("SELECT ID FROM instagram_stats WHERE Username = %s", (row['Username'],))
    instagram_stats_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO followers_history (InstagramStatsID, FollowersCount, RecordedAt)
        VALUES (%s, %s, NOW())
    """, (instagram_stats_id, row['Followers']))
    cnx.commit()

    cursor.execute("""
        INSERT INTO engagement_history (InstagramStatsID, EngagementRate, RecordedAt)
        VALUES (%s, %s, NOW())
    """, (instagram_stats_id, row['Engagement']))
    cnx.commit()

# Close the connection
cursor.close()
cnx.close()