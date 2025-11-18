-- schema.sql
CREATE DATABASE IF NOT EXISTS client_query_db;
USE client_query_db;

-- client_queries table per PDF columns
CREATE TABLE IF NOT EXISTS client_queries (
    query_id INT PRIMARY KEY AUTO_INCREMENT,
    mail_id VARCHAR(255) NOT NULL,
    mobile_number VARCHAR(30),
    query_heading VARCHAR(255),
    query_description TEXT,
    status ENUM('Open','Closed') DEFAULT 'Open',
    query_created_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    query_closed_time DATETIME NULL,
    UNIQUE KEY uniq_mail_heading (mail_id, query_heading(120))
);

-- users table for login system (username, hashed_password, role)
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(150) NOT NULL UNIQUE,
    hashed_password VARCHAR(255) NOT NULL,
    role ENUM('Client','Support') NOT NULL
);
