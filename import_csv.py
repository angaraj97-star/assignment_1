#!/usr/bin/env python3
"""
import_csv.py - Importer that accepts your CSV column names and maps them to DB schema.

Accepted CSV headers (this script recognizes these variants):
  - query_id
  - mail_id  OR client_email
  - mobile_number OR client_mobile
  - query_heading
  - query_description
  - status (Open, Closed, In Progress, Resolved)
  - query_created_time OR date_raised
  - query_closed_time OR date_closed

Behavior:
  - If query_id is missing/blank -> generate uuid4 hex (<=64 chars)
  - Validates required fields (mail_id/client_email, query_heading, query_description, status, date_raised/query_created_time)
  - Creates client_queries table if missing (schema without assigned_to)
  - Upserts rows using ON DUPLICATE KEY UPDATE
  - Reads DB credentials from .env via python-dotenv (DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT)
"""

from dotenv import load_dotenv
load_dotenv()

import os
import argparse
import uuid
from datetime import datetime
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

# Allowed statuses in your schema
ALLOWED_STATUSES = {'Open', 'Closed', 'In Progress', 'Resolved'}

# Acceptable incoming header synonyms -> target header names
HEADER_MAP = {
    'query_id': 'query_id',
    'mail_id': 'mail_id',
    'client_email': 'mail_id',
    'mobile_number': 'mobile_number',
    'client_mobile': 'mobile_number',
    'query_heading': 'query_heading',
    'query_description': 'query_description',
    'status': 'status',
    'query_created_time': 'query_created_time',
    'date_raised': 'query_created_time',
    'query_closed_time': 'query_closed_time',
    'date_closed': 'query_closed_time'
}

# Which target columns are required in normalized dataframe
REQUIRED_TARGET_COLS = {'mail_id', 'query_heading', 'query_description', 'status', 'query_created_time'}

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASS', ''),
    'database': os.environ.get('DB_NAME', 'client_query_db'),
    'port': int(os.environ.get('DB_PORT', 3306))
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS client_queries (
    query_id VARCHAR(64) PRIMARY KEY,
    mail_id VARCHAR(255) NOT NULL,
    mobile_number VARCHAR(50),
    query_heading VARCHAR(255),
    query_description TEXT,
    status ENUM('Open','Closed','In Progress','Resolved') DEFAULT 'Open',
    query_created_time DATETIME,
    query_closed_time DATETIME NULL
)
"""

INSERT_SQL = """
INSERT INTO client_queries
(query_id, mail_id, mobile_number, query_heading, query_description, status, query_created_time, query_closed_time)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  mail_id=VALUES(mail_id),
  mobile_number=VALUES(mobile_number),
  query_heading=VALUES(query_heading),
  query_description=VALUES(query_description),
  status=VALUES(status),
  query_created_time=VALUES(query_created_time),
  query_closed_time=VALUES(query_closed_time)
"""

def connect_db(cfg):
    try:
        return mysql.connector.connect(**cfg)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            # create DB then reconnect
            tmp = cfg.copy()
            tmp.pop('database', None)
            cn = mysql.connector.connect(**tmp)
            cur = cn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            cn.commit()
            cur.close()
            cn.close()
            return mysql.connector.connect(**cfg)
        raise

def parse_datetime_safe(v):
    if pd.isna(v) or v is None or str(v).strip() == '':
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    fmts = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y")
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            continue
    try:
        return pd.to_datetime(s).to_pydatetime()
    except Exception:
        return None

def normalize_status(s):
    if pd.isna(s) or s is None:
        return 'Open'
    s_val = str(s).strip()
    for allowed in ALLOWED_STATUSES:
        if s_val.lower() == allowed.lower():
            return allowed
    return 'Open'

def ensure_table_exists():
    cnx = connect_db(DB_CONFIG)
    cur = cnx.cursor()
    cur.execute(CREATE_TABLE_SQL)
    cnx.commit()
    cur.close()
    cnx.close()

def map_and_normalize_columns(df):
    # Lower-case column names trimmed for robust mapping
    col_map = {}
    for c in df.columns:
        key = c.strip()
        if key in HEADER_MAP:
            col_map[c] = HEADER_MAP[key]
    # Rename cols found
    df = df.rename(columns=col_map)
    # identify missing required targets
    missing = REQUIRED_TARGET_COLS - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns after mapping: {missing}. Found columns: {list(df.columns)}")
    # Warn about extras (ignored)
    # Normalize column ordering
    return df

def import_csv(csv_path):
    # read CSV as strings to avoid coercion surprises
    df = pd.read_csv(csv_path, dtype=str)
    df = map_and_normalize_columns(df)

    # Ensure table exists
    ensure_table_exists()

    cnx = connect_db(DB_CONFIG)
    cur = cnx.cursor()
    inserted = 0

    for _, row in df.iterrows():
        # query_id: use provided or generate UUID4 hex (<=64 chars)
        qid_raw = row.get('query_id', None)
        if pd.isna(qid_raw) or not str(qid_raw).strip():
            qid = uuid.uuid4().hex[:64]
        else:
            qid = str(qid_raw).strip()[:64]

        mail = str(row.get('mail_id', '')).strip() or None
        mobile = str(row.get('mobile_number', '')).strip() or None
        heading = str(row.get('query_heading', '')).strip() or None
        desc = str(row.get('query_description', '')).strip() or None
        status = normalize_status(row.get('status', None))
        created = parse_datetime_safe(row.get('query_created_time', None))
        closed = parse_datetime_safe(row.get('query_closed_time', None))

        cur.execute(INSERT_SQL, (qid, mail, mobile, heading, desc, status, created, closed))
        inserted += 1

    cnx.commit()
    cur.close()
    cnx.close()
    print(f"Imported / upserted {inserted} rows into client_queries.")

def main():
    parser = argparse.ArgumentParser(description="Import CSV into client_queries (column-mapped importer)")
    parser.add_argument("csv", help="Path to CSV file to import")
    args = parser.parse_args()
    import_csv(args.csv)

if __name__ == "__main__":
    main()
