import requests
import pandas as pd
import mysql.connector
from datetime import datetime
from config.config import API_KEY, CITY, DB_CONFIG

def fetch_weather_data():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    
    response = requests.get(url)
    data = response.json()

    weather_data = {
        "city": CITY,
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather_condition": data["weather"][0]["description"],
        "timestamp": datetime.now()
    }

    return pd.DataFrame([weather_data])

def insert_into_db(df):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
    INSERT INTO weather_data (city, temperature, humidity, weather_condition, timestamp)
    VALUES (%s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cursor.execute(query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

def main():
    df = fetch_weather_data()
    insert_into_db(df)
    print("Weather data inserted successfully!")

if __name__ == "__main__":
    main()
