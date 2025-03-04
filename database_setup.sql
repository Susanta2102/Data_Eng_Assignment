-- Create database
CREATE DATABASE IF NOT EXISTS company_data;

-- Create tables
USE company_data;

CREATE TABLE common_crawl_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    url VARCHAR(512) NOT NULL,
    company_name VARCHAR(255),
    industry VARCHAR(255),
    crawl_date DATE DEFAULT (CURRENT_DATE)
);

CREATE TABLE abr_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    abn VARCHAR(11) NOT NULL,
    company_name VARCHAR(255),
    entity_type VARCHAR(50),
    state VARCHAR(3),
    postcode VARCHAR(4),
    registration_date DATE
);

CREATE TABLE companies (
    company_id INT AUTO_INCREMENT PRIMARY KEY,
    url VARCHAR(512) NOT NULL UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    industry VARCHAR(255),
    abn VARCHAR(11),
    entity_type VARCHAR(50),
    state VARCHAR(3),
    postcode VARCHAR(4),
    registration_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user
CREATE USER 'pipeline_user'@'localhost' IDENTIFIED BY 'secure_password';
GRANT ALL PRIVILEGES ON company_data.* TO 'pipeline_user'@'localhost';
FLUSH PRIVILEGES;