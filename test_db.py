from dotenv import load_dotenv
load_dotenv()

import os
import mysql.connector

cfg = {
    'host': os.environ.get('DB_HOST'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASS'),
    'database': os.environ.get('DB_NAME'),
    'port': int(os.environ.get('DB_PORT')),
}

try:
    conn = mysql.connector.connect(**cfg)
    print("Connected!", conn)
    conn.close()
except Exception as e:
    print("Error:", e)
