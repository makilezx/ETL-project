-- Create the metabase db, granting privileges
CREATE DATABASE metabase;
GRANT ALL PRIVILEGES ON DATABASE metabase TO myuser;

-- (Used for testing purposes. Not necessary anymore, already defined in the docker-compose.yml)
-- CREATE DATABASE user_db;

-- Switch to the user_db
\c user_db;

-- Create schema
CREATE SCHEMA IF NOT EXISTS user_schema;

-- Create tables
-- Necessary step because of idea to ingest transformed data into already existing tables

CREATE TABLE IF NOT EXISTS user_schema.user (
    pid VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    gender VARCHAR(255),
    measure_code VARCHAR(255),
    rating VARCHAR(255));

CREATE TABLE IF NOT EXISTS user_schema.earnings (
    earnings_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pid VARCHAR(255),
    user_id VARCHAR(255),
    earnings_in_thousands FLOAT,
    price_per_hour FLOAT,
    FOREIGN KEY (pid) REFERENCES user_schema.user (pid));

CREATE TABLE IF NOT EXISTS user_schema.jobs (
    jobs_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pid VARCHAR(255),
    user_id VARCHAR(255),
    total_hours INT,
    job_success_perc FLOAT,
    main_profession VARCHAR(255),
    job_title VARCHAR(255),
    completed_jobs INT,
    FOREIGN KEY (pid) REFERENCES user_schema.user (pid));

CREATE TABLE IF NOT EXISTS user_schema.geo (
    geo_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pid VARCHAR(255),
    user_id VARCHAR(255),
    country VARCHAR(255),
    city VARCHAR(255),
    region VARCHAR(255),
    country_code VARCHAR(255),
    FOREIGN KEY (pid) REFERENCES user_schema.user (pid));

-- Privileges/permissions
GRANT ALL PRIVILEGES ON SCHEMA user_schema TO myuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA user_schema TO myuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA user_schema TO myuser;