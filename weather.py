import requests
import sqlite3
from collections import Counter

API_KEY = "8ca692062a27bfb1caa7369a23190450"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
cities = ["London", "New York", "Tokyo", "Paris", "Berlin"]

DB_NAME = 'weather_data.db'

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

def create_table():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            city TEXT,
            temperature REAL,
            humidity REAL,
            pressure REAL,
            weather_description TEXT
        )
    ''')
    conn.commit()

def fetch_weather_data():
    for city in cities:
        params = {
            'q': city,
            'appid': API_KEY,
            'units': 'metric'
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            city_name = data["name"]
            temperature = data["main"]["temp"]
            humidity = data["main"]["humidity"]
            pressure = data["main"]["pressure"]
            description = data["weather"][0]["description"]
            cursor.execute('''
                INSERT INTO weather (city, temperature, humidity, pressure, weather_description)
                VALUES (?, ?, ?, ?, ?)
            ''', (city_name, temperature, humidity, pressure, description))
            conn.commit()
            print(f"Data for {city_name} inserted into database.")
        else:
            print(f"Error fetching data for {city}: {response.status_code} - {response.json().get('message')}")

def perform_data_analytics():
    print("\nPerforming Data Analytics...")
    cursor.execute('SELECT AVG(temperature) FROM weather')
    avg_temp = cursor.fetchone()[0]
    if avg_temp is None:
        print("No temperature data available to calculate average.")
    else:
        print(f"Average temperature across all cities: {avg_temp:.2f}°C")
    cursor.execute('SELECT weather_description FROM weather')
    descriptions = [row[0] for row in cursor.fetchall()]
    weather_counter = Counter(descriptions)
    print("\nTop 3 Most Common Weather Descriptions:")
    for description, count in weather_counter.most_common(3):
        print(f"{description}: {count} occurrences")
    cursor.execute('SELECT city, temperature FROM weather ORDER BY temperature DESC LIMIT 5')
    top_5_hot_cities = cursor.fetchall()
    print("\nTop 5 Cities with Highest Temperatures:")
    for city, temp in top_5_hot_cities:
        print(f"{city}: {temp:.2f}°C")

def main():
    create_table()
    fetch_weather_data()
    perform_data_analytics()

if __name__ == "__main__":
    main()
    conn.close()
