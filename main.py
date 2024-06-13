from flask import Flask
import pyodbc
from db_connect import create_table
from download_load import fetch_and_queue_zip_files
from worker import stop_threads

app = Flask(__name__)


create_table()

if __name__ == '__main__':
    app.run(debug=True)
    try:
        app.run(debug=True)
    finally:
        stop_threads()

