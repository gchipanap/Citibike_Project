import pyodbc

# conn = pyodbc.connect('DRIVER={SQL Server};Server=DESKTOP-HROJ9KU;Database=c_b')
# print('db_conected')

def create_connection():
    try:
        conn = pyodbc.connect('DRIVER={SQL Server};Server=DESKTOP-HROJ9KU;Database=c_b')
        return conn
    except Exception as e:
        print(e)

conn = create_connection()

def table_exists(cursor, table_name):
    cursor.execute(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
    return cursor.fetchone() is not None

def create_table():
    cursor = conn.cursor()
    try:
        if not table_exists(cursor, 'Region'):
            cursor.execute('''
                CREATE TABLE Region (
                    id INT PRIMARY KEY,
                    name NVARCHAR(50),
                );
            ''')
            conn.commit()
        if not table_exists(cursor, 'Station'):
            cursor.execute('''
                CREATE TABLE Station (
                    name NVARCHAR(50),
                    lat FLOAT,
                    region_id INT,
                    capacity INT,
                    station_id NVARCHAR(60) PRIMARY KEY,
                    short_name NVARCHAR(10),
                    lon FLOAT,
                    FOREIGN KEY (region_id) REFERENCES Region(id)
                );
            ''')
            conn.commit()

        if not table_exists(cursor, 'Vehicle'):
            cursor.execute('''
                CREATE TABLE Vehicle (
                    form_factor NVARCHAR(50),
                    vehicle_type_id INT PRIMARY KEY,
                    propulsion_type  NVARCHAR(30),
                    max_range_meters FLOAT
                );
            ''')
            conn.commit()
        if not table_exists(cursor, 'TripHistory'):
            cursor.execute('''
                CREATE TABLE TripHistory (
                    ride_id INT PRIMARY KEY,
                    rideable_type NVARCHAR(20),
                    start_time NVARCHAR(20) NOT NULL,
                    end_time NVARCHAR(20) NOT NULL,
                    start_station_name VARCHAR(255) NOT NULL,
                    end_station_name VARCHAR(255) NOT NULL,
                    start_station_id VARCHAR(255) NOT NULL,
                    end_station_id VARCHAR(255) NOT NULL,
                    start_latitude FLOAT NOT NULL,
                    start_longitude FLOAT NOT NULL,
                    end_latitude FLOAT NOT NULL,
                    end_longitude FLOAT NOT NULL,
                    user_type VARCHAR(20) NOT NULL
                    );
                ''')
            conn.commit()
        if not table_exists(cursor, 'StationStatus'):
            cursor.execute('''
                CREATE TABLE StationStatus (
                    num_bikes_disabled INT,
                    last_reported INT,
                    vehicle_types_available_type_1 INT,
                    vehicle_types_available_type_2 INT,
                    station_id NVARCHAR(60) PRIMARY KEY,
                    FOREIGN KEY (station_id) REFERENCES Station(station_id)
                );
            ''')
            conn.commit()
    except Exception as e:
        print('Error al crear la tabla:', e)
    finally:
        cursor.close()
