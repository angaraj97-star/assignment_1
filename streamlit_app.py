#!/usr/bin/env python3
"""
Streamlit app adapted to the user database schema:
- client_queries.query_id : VARCHAR(64) PK
- status: ENUM('Open','Closed','In Progress','Resolved')
- assigned_to supported
- users table has created_at

Features:
- Register / Login with SHA256 hashed passwords (users.role in ['Client','Support'])
- Client: submit new query (generates uuid query_id if not provided)
- Support: list/filter queries, change status, assign queries to support users
- When status is changed to 'Resolved' or 'Closed' -> set query_closed_time to now
"""
from dotenv import load_dotenv
load_dotenv()

import os
import hashlib
import uuid
from datetime import datetime
import pandas as pd
import streamlit as st
import mysql.connector

# DB config via env
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASS', ''),
    'database': os.environ.get('DB_NAME', 'client_query_db'),
    'port': int(os.environ.get('DB_PORT', 3306))
}

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def ensure_tables():
    cnx = get_conn()
    cur = cnx.cursor()
    # users table (matches your schema)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY AUTO_INCREMENT,
        username VARCHAR(100) NOT NULL UNIQUE,
        hashed_password VARCHAR(256) NOT NULL,
        role ENUM('Client','Support') NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    # client_queries (matches your schema)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS client_queries (
        query_id VARCHAR(64) PRIMARY KEY,
        mail_id VARCHAR(255) NOT NULL,
        mobile_number VARCHAR(50),
        query_heading VARCHAR(255),
        query_description TEXT,
        status ENUM('Open','Closed','In Progress','Resolved') DEFAULT 'Open',
        query_created_time DATETIME,
        query_closed_time DATETIME NULL,
        assigned_to VARCHAR(150)
    )""")
    cnx.commit()
    cur.close()
    cnx.close()

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def register_user(username, password, role):
    hashed = hash_password(password)
    cnx = get_conn()
    cur = cnx.cursor()
    try:
        cur.execute("INSERT INTO users (username, hashed_password, role) VALUES (%s,%s,%s)", (username, hashed, role))
        cnx.commit()
        return True
    except mysql.connector.IntegrityError:
        return False
    finally:
        cur.close()
        cnx.close()

def authenticate(username, password):
    hashed = hash_password(password)
    cnx = get_conn()
    cur = cnx.cursor(dictionary=True)
    cur.execute("SELECT user_id, username, role FROM users WHERE username=%s AND hashed_password=%s", (username, hashed))
    row = cur.fetchone()
    cur.close()
    cnx.close()
    return row

def submit_query(mail, mobile, heading, desc, query_id=None, assigned_to=None):
    cnx = get_conn()
    cur = cnx.cursor()
    if not query_id:
        query_id = uuid.uuid4().hex[:64]
    now = datetime.now()
    cur.execute("""
        INSERT INTO client_queries
        (query_id, mail_id, mobile_number, query_heading, query_description, status, query_created_time, assigned_to)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            mail_id=VALUES(mail_id),
            mobile_number=VALUES(mobile_number),
            query_heading=VALUES(query_heading),
            query_description=VALUES(query_description),
            status=VALUES(status),
            query_created_time=VALUES(query_created_time),
            assigned_to=VALUES(assigned_to)
    """, (query_id, mail, mobile, heading, desc, 'Open', now, assigned_to))
    cnx.commit()
    cur.close()
    cnx.close()
    return query_id

def load_queries(status=None):
    cnx = get_conn()
    if status in ('Open','Closed','In Progress','Resolved'):
        df = pd.read_sql(f"SELECT * FROM client_queries WHERE status = %s ORDER BY query_created_time DESC", cnx, params=(status,))
    else:
        df = pd.read_sql("SELECT * FROM client_queries ORDER BY query_created_time DESC", cnx)
    cnx.close()
    return df

def list_support_usernames():
    cnx = get_conn()
    cur = cnx.cursor()
    cur.execute("SELECT username FROM users WHERE role='Support'")
    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return [r[0] for r in rows]

def update_query_status_and_assign(qid, new_status, assigned_to):
    cnx = get_conn()
    cur = cnx.cursor()
    now = datetime.now()
    # If marking Resolved or Closed, set closed time; if reopening, NULL it
    if new_status in ('Resolved','Closed'):
        cur.execute("UPDATE client_queries SET status=%s, assigned_to=%s, query_closed_time=%s WHERE query_id=%s",
                    (new_status, assigned_to, now, qid))
    else:
        cur.execute("UPDATE client_queries SET status=%s, assigned_to=%s, query_closed_time=NULL WHERE query_id=%s",
                    (new_status, assigned_to, qid))
    cnx.commit()
    cur.close()
    cnx.close()

def compute_metrics(df):
    total = len(df)
    open_count = len(df[df['status']=='Open'])
    closed_count = len(df[df['status'].isin(['Closed','Resolved'])])
    avg_resolution = None
    closed_df = df[df['status'].isin(['Closed','Resolved'])].copy()
    if not closed_df.empty and 'query_closed_time' in closed_df.columns:
        closed_df['query_created_time'] = pd.to_datetime(closed_df['query_created_time'])
        closed_df['query_closed_time'] = pd.to_datetime(closed_df['query_closed_time'])
        closed_df = closed_df.dropna(subset=['query_closed_time'])
        if not closed_df.empty:
            closed_df['resolution_seconds'] = (closed_df['query_closed_time'] - closed_df['query_created_time']).dt.total_seconds()
            avg_secs = closed_df['resolution_seconds'].mean()
            if pd.notna(avg_secs):
                avg_resolution = pd.to_timedelta(avg_secs, unit='s')
    return {'total': total, 'open': open_count, 'closed': closed_count, 'avg_resolution': avg_resolution}

# --- Streamlit UI ---
st.set_page_config(page_title="Client Query Management (Adapted Schema)")
ensure_tables()

if 'user' not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.title("Login / Register")
    tab = st.tabs(["Login", "Register"])
    with tab[0]:
        st.subheader("Login")
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type='password')
            submitted = st.form_submit_button("Login")
        if submitted:
            user = authenticate(username, password)
            if user:
                st.session_state.user = user
                st.success(f"Logged in as {user['username']} ({user['role']})")
                st.experimental_rerun()
            else:
                st.error("Invalid credentials.")

    with tab[1]:
        st.subheader("Register")
        with st.form("reg_form"):
            r_user = st.text_input("Choose username")
            r_pass = st.text_input("Choose password", type='password')
            r_role = st.selectbox("Role", ["Client","Support"])
            r_sub = st.form_submit_button("Register")
        if r_sub:
            if not r_user or not r_pass:
                st.warning("Must provide username and password.")
            else:
                ok = register_user(r_user, r_pass, r_role)
                if ok:
                    st.success("Registered successfully. You can now login.")
                else:
                    st.error("Username already exists. Choose another.")

else:
    user = st.session_state.user
    st.sidebar.write(f"Signed in as: **{user['username']}** ({user['role']})")
    if st.sidebar.button("Logout"):
        st.session_state.user = None
        st.experimental_rerun()

    if user['role'] == 'Client':
        st.title("Client — Submit a Query")
        with st.form("client_query_form"):
            mail = st.text_input("Email ID")
            mobile = st.text_input("Mobile Number")
            heading = st.text_input("Query Heading")
            desc = st.text_area("Query Description")
            submit = st.form_submit_button("Submit Query")
        if submit:
            if not mail or not heading:
                st.warning("Please provide Email and Query Heading.")
            else:
                qid = submit_query(mail, mobile, heading, desc)
                st.success(f"Query submitted (ID: {qid}). It is marked Open.")
    else:
        st.title("Support Dashboard")
        st.markdown("Filter and manage client queries.")
        status_filter = st.selectbox("Status filter", ["All","Open","In Progress","Resolved","Closed"])
        df = load_queries(None if status_filter == "All" else status_filter)
        metrics = compute_metrics(df)
        st.metric("Total queries", metrics['total'])
        st.metric("Open", metrics['open'])
        st.metric("Closed/Resolved", metrics['closed'])
        if metrics['avg_resolution']:
            st.write("Average resolution time (closed/resolved):", metrics['avg_resolution'])

        st.write("Query table (newest first):")
        st.dataframe(df)

        st.write("---")
        st.write("Update a query (assign / change status):")
        if df.empty:
            st.info("No queries to manage.")
        else:
            qid = st.selectbox("Choose Query ID", df['query_id'].tolist())
            selected_row = df[df['query_id'] == qid].iloc[0]
            st.write("Current status:", selected_row['status'])
            # list of statuses to choose from
            new_status = st.selectbox("New status", ["Open", "In Progress", "Resolved", "Closed"], index=["Open","In Progress","Resolved","Closed"].index(selected_row['status']) if selected_row['status'] in ["Open","In Progress","Resolved","Closed"] else 0)
            support_list = list_support_usernames()
            if not support_list:
                st.info("No support users found. Register a support user first.")
                assigned = st.text_input("Assign to (free text)")
            else:
                assigned = st.selectbox("Assign to", ["(none)"] + support_list)
                if assigned == "(none)":
                    assigned = None

            if st.button("Apply update"):
                update_query_status_and_assign(qid, new_status, assigned)
                st.success(f"Updated query {qid}: status -> {new_status}, assigned_to -> {assigned}")
                st.experimental_rerun()
