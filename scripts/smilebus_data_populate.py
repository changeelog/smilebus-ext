import sqlite3
import requests
import logging
import concurrent.futures
import json
from datetime import datetime

logging.basicConfig(filename='smilebus_db_population.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def create_database():
    """
    Creates an optimized SQLite database with tables for cities, stops, and routes.
    """
    conn = sqlite3.connect('smilebus.db')
    c = conn.cursor()

    c.executescript('''
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            slug TEXT,
            is_waypoint BOOLEAN,
            is_top BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS stops (
            id INTEGER PRIMARY KEY,
            city_id INTEGER,
            title TEXT NOT NULL,
            gps TEXT,
            photo_url TEXT,
            order_num INTEGER,
            is_default BOOLEAN,
            FOREIGN KEY (city_id) REFERENCES cities (id)
        );

        CREATE TABLE IF NOT EXISTS routes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_city_id INTEGER,
            to_city_id INTEGER,
            from_stop_id INTEGER,
            to_stop_id INTEGER,
            FOREIGN KEY (from_city_id) REFERENCES cities (id),
            FOREIGN KEY (to_city_id) REFERENCES cities (id),
            FOREIGN KEY (from_stop_id) REFERENCES stops (id),
            FOREIGN KEY (to_stop_id) REFERENCES stops (id)
        );

        CREATE INDEX IF NOT EXISTS idx_cities_slug ON cities (slug);
        CREATE INDEX IF NOT EXISTS idx_stops_city_id ON stops (city_id);
        CREATE INDEX IF NOT EXISTS idx_routes_from_to ON routes (from_city_id, to_city_id);
    ''')

    conn.commit()
    conn.close()

def fetch_city_data(city_id):
    """
    Fetches city data from the SmileBus API for a given city ID.
    
    Args:
    city_id (int): The ID of the city to fetch data for.
    
    Returns:
    dict: A dictionary containing city data, or None if the request fails.
    """
    url = f"https://smilebus.by/api/v2/route/cities-arrival?city_from_id={city_id}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data['result'] == 'success' and data['data']:
            return {'city_id': city_id, 'data': data['data']}
    except requests.RequestException as e:
        logging.error(f"Error fetching data for city ID {city_id}: {str(e)}")
    return None

def process_city_data(conn, city_data):
    """
    Processes and inserts city data into the database.
    
    Args:
    conn (sqlite3.Connection): The database connection.
    city_data (dict): A dictionary containing city data.
    """
    c = conn.cursor()
    for city in city_data['data']:
        c.execute('''
            INSERT OR REPLACE INTO cities (id, name, slug, is_waypoint, is_top)
            VALUES (?, ?, ?, ?, ?)
        ''', (city['id_city'], city['city_name'], city['city_slug'],
              bool(city['is_waypoint']), bool(city['is_top'])))

        for stop in city['stops']:
            c.execute('''
                INSERT OR REPLACE INTO stops (id, city_id, title, gps, photo_url, order_num, is_default)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (stop['id'], city['id_city'], stop['title'], stop['gps'],
                  stop['photo_url'], stop['order'], bool(stop['is_default'])))

        # Insert routes
        for stop in city['stops']:
            c.execute('''
                INSERT OR IGNORE INTO routes (from_city_id, to_city_id, from_stop_id, to_stop_id)
                VALUES (?, ?, ?, ?)
            ''', (city_data['city_id'], city['id_city'], None, stop['id']))

def populate_database():
    """
    Main function to populate the database with data from the SmileBus API.
    Uses multi-threading for faster data fetching and processing.
    """
    conn = sqlite3.connect('smilebus.db')
    conn.execute('PRAGMA journal_mode = WAL')  # Optimize for concurrent writes
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_city = {executor.submit(fetch_city_data, city_id): city_id for city_id in range(1, 1001)}
            
            for future in concurrent.futures.as_completed(future_to_city):
                city_id = future_to_city[future]
                try:
                    city_data = future.result()
                    if city_data:
                        process_city_data(conn, city_data)
                        logging.info(f"Processed data for city ID {city_id}")
                except Exception as e:
                    logging.error(f"Error processing city ID {city_id}: {str(e)}")
        
        conn.commit()
        logging.info("Database population completed successfully")
    
    except Exception as e:
        logging.error(f"Fatal error during database population: {str(e)}")
    
    finally:
        conn.close()

def export_route_summary(output_file='route_summary.json'):
    """
    Exports a summary of routes in a more informative and convenient format.
    
    Args:
    output_file (str): The name of the output JSON file.
    """
    conn = sqlite3.connect('smilebus.db')
    c = conn.cursor()

    c.execute('''
        SELECT 
            r.id,
            c1.name AS from_city,
            c2.name AS to_city,
            s1.title AS from_stop,
            s2.title AS to_stop
        FROM routes r
        JOIN cities c1 ON r.from_city_id = c1.id
        JOIN cities c2 ON r.to_city_id = c2.id
        LEFT JOIN stops s1 ON r.from_stop_id = s1.id
        JOIN stops s2 ON r.to_stop_id = s2.id
        ORDER BY c1.name, c2.name, s1.title, s2.title
    ''')

    routes = [
        {
            'id': row[0],
            'from_city': row[1],
            'to_city': row[2],
            'from_stop': row[3] or 'Any',
            'to_stop': row[4]
        }
        for row in c.fetchall()
    ]

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(routes, f, ensure_ascii=False, indent=2)

    logging.info(f"Route summary exported to {output_file}")

if __name__ == "__main__":
    """
    Main execution block. Creates the database, populates it with data,
    and exports a route summary.
    """
    start_time = datetime.now()
    try:
        create_database()
        populate_database()
        export_route_summary()
        end_time = datetime.now()
        logging.info(f"Total execution time: {end_time - start_time}")
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")

def analyze_database():
    """
    Analyzes the populated database and prints useful statistics.
    """
    conn = sqlite3.connect('smilebus.db')
    c = conn.cursor()

    print("Database Analysis:")
    print("-----------------")

    # Count cities
    c.execute("SELECT COUNT(*) FROM cities")
    city_count = c.fetchone()[0]
    print(f"Total cities: {city_count}")

    # Count stops
    c.execute("SELECT COUNT(*) FROM stops")
    stop_count = c.fetchone()[0]
    print(f"Total stops: {stop_count}")

    # Count routes
    c.execute("SELECT COUNT(*) FROM routes")
    route_count = c.fetchone()[0]
    print(f"Total routes: {route_count}")

    # Top 5 cities with most stops
    c.execute("""
        SELECT c.name, COUNT(s.id) as stop_count
        FROM cities c
        JOIN stops s ON c.id = s.city_id
        GROUP BY c.id
        ORDER BY stop_count DESC
        LIMIT 5
    """)
    print("\nTop 5 cities with most stops:")
    for city, count in c.fetchall():
        print(f"  {city}: {count} stops")

    # Top 5 most connected cities (cities with most routes)
    c.execute("""
        SELECT c.name, COUNT(*) as route_count
        FROM cities c
        JOIN routes r ON c.id = r.from_city_id
        GROUP BY c.id
        ORDER BY route_count DESC
        LIMIT 5
    """)
    print("\nTop 5 most connected cities:")
    for city, count in c.fetchall():
        print(f"  {city}: {count} routes")

    conn.close()

def optimize_database():
    """
    Performs database optimization after population.
    """
    conn = sqlite3.connect('smilebus.db')
    c = conn.cursor()

    logging.info("Starting database optimization...")

    c.executescript('''
        PRAGMA optimize;
        ANALYZE;
        VACUUM;
    ''')

    logging.info("Database optimization completed.")
    conn.close()

if __name__ == "__main__":
    """
    Main execution block. Creates the database, populates it with data,
    exports a route summary, analyzes the database, and performs optimization.
    """
    start_time = datetime.now()
    try:
        create_database()
        populate_database()
        export_route_summary()
        analyze_database()
        optimize_database()
        end_time = datetime.now()
        logging.info(f"Total execution time: {end_time - start_time}")
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
