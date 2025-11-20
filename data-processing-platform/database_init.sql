-- Database Initialization Script for Data Processing Platform
-- This script creates all necessary tables and initializes sample data

-- Create the database (if not exists)
-- Note: This requires superuser privileges in PostgreSQL
-- CREATE DATABASE data_platform;

-- Connect to the database
-- \c data_platform;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(100) DEFAULT 'user',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Create data_sources table
CREATE TABLE IF NOT EXISTS data_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    source_type VARCHAR(100) NOT NULL, -- postgres, mysql, csv, parquet, etc.
    connection_config JSONB NOT NULL,
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Create queries table
CREATE TABLE IF NOT EXISTS queries (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    data_source_id INTEGER REFERENCES data_sources(id),
    sql_text TEXT NOT NULL,
    query_name VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create query_results table
CREATE TABLE IF NOT EXISTS query_results (
    id SERIAL PRIMARY KEY,
    query_id INTEGER REFERENCES queries(id),
    result_data JSONB,
    execution_time_ms BIGINT,
    status VARCHAR(50) DEFAULT 'completed',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create casbin_rules table for Casbin policy storage
CREATE TABLE IF NOT EXISTS casbin_rules (
    id SERIAL PRIMARY KEY,
    ptype VARCHAR(100),
    v0 TEXT,
    v1 TEXT,
    v2 TEXT,
    v3 TEXT,
    v4 TEXT,
    v5 TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_data_sources_type ON data_sources(source_type);
CREATE INDEX IF NOT EXISTS idx_queries_user_id ON queries(user_id);
CREATE INDEX IF NOT EXISTS idx_query_results_query_id ON query_results(query_id);
CREATE INDEX IF NOT EXISTS idx_casbin_rules_ptype ON casbin_rules(ptype);

-- Insert sample users (password is 'password' hashed with bcrypt)
INSERT INTO users (username, email, password_hash, role, is_active) 
VALUES 
    ('admin', 'admin@example.com', '$2b$12$LQv3c158Q4YZW6z7z2P7k.hLm3n4o5p6q7r8s9t0u1v2w3x4y5z6', 'admin', TRUE),
    ('user1', 'user1@example.com', '$2b$12$LQv3c158Q4YZW6z7z2P7k.hLm3n4o5p6q7r8s9t0u1v2w3x4y5z6', 'user', TRUE),
    ('data_analyst', 'analyst@example.com', '$2b$12$LQv3c158Q4YZW6z7z2P7k.hLm3n4o5p6q7r8s9t0u1v2w3x4y5z6', 'analyst', TRUE)
ON CONFLICT (username) DO NOTHING;

-- Insert sample data sources
INSERT INTO data_sources (name, description, source_type, connection_config, created_by) 
VALUES 
    ('PostgreSQL Production DB', 'Main production PostgreSQL database', 'postgres', 
     '{"host": "prod-db.example.com", "port": 5432, "database": "production", "username": "service_user"}', 1),
    ('MySQL Analytics DB', 'Analytics database for reporting', 'mysql',
     '{"host": "analytics-db.example.com", "port": 3306, "database": "analytics", "username": "analytics_user"}', 1),
    ('CSV Data Files', 'Directory of CSV files for batch processing', 'csv',
     '{"path": "/data/csv_files", "delimiter": ",", "header": true}', 1),
    ('Parquet Data Lake', 'Apache Parquet files in data lake', 'parquet',
     '{"path": "/data/parquet", "format": "parquet"}', 1)
ON CONFLICT (name) DO NOTHING;

-- Insert sample queries
INSERT INTO queries (user_id, data_source_id, sql_text, query_name, description) 
VALUES 
    (1, 1, 'SELECT * FROM users LIMIT 10;', 'Get Users Sample', 'Sample query to retrieve users'),
    (2, 1, 'SELECT COUNT(*) FROM users;', 'Count All Users', 'Count total number of users'),
    (3, 2, 'SELECT * FROM sales_data WHERE date > ''2023-01-01'';', 'Recent Sales', 'Get sales data from 2023')
ON CONFLICT (id) DO NOTHING;

-- Insert sample Casbin policies
INSERT INTO casbin_rules (ptype, v0, v1, v2) 
VALUES 
    ('p', 'admin', '*', '*'),           -- Admin can access all resources
    ('p', 'user', 'own', 'read'),       -- Regular users can read their own data
    ('p', 'analyst', 'reports', 'read'), -- Analysts can read reports
    ('g', 'user1', 'user'),             -- user1 has user role
    ('g', 'data_analyst', 'analyst')    -- data_analyst has analyst role
ON CONFLICT (id) DO NOTHING;

-- Create a function to refresh updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update the updated_at column
CREATE TRIGGER update_users_updated_at 
    BEFORE UPDATE ON users 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_data_sources_updated_at 
    BEFORE UPDATE ON data_sources 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_queries_updated_at 
    BEFORE UPDATE ON queries 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Grant necessary permissions (adjust as needed for your security requirements)
-- GRANT ALL PRIVILEGES ON TABLE users TO data_platform_user;
-- GRANT ALL PRIVILEGES ON TABLE data_sources TO data_platform_user;
-- GRANT ALL PRIVILEGES ON TABLE queries TO data_platform_user;
-- GRANT ALL PRIVILEGES ON TABLE query_results TO data_platform_user;
-- GRANT ALL PRIVILEGES ON TABLE casbin_rules TO data_platform_user;

-- Create sequences for proper ID generation if needed
-- CREATE SEQUENCE IF NOT EXISTS users_id_seq START 1;
-- CREATE SEQUENCE IF NOT EXISTS data_sources_id_seq START 1;
-- CREATE SEQUENCE IF NOT EXISTS queries_id_seq START 1;
-- CREATE SEQUENCE IF NOT EXISTS query_results_id_seq START 1;
-- CREATE SEQUENCE IF NOT EXISTS casbin_rules_id_seq START 1;

-- Associate sequences with tables if needed
-- ALTER TABLE users ALTER COLUMN id SET DEFAULT nextval('users_id_seq');
-- ALTER TABLE data_sources ALTER COLUMN id SET DEFAULT nextval('data_sources_id_seq');
-- ALTER TABLE queries ALTER COLUMN id SET DEFAULT nextval('queries_id_seq');
-- ALTER TABLE query_results ALTER COLUMN id SET DEFAULT nextval('query_results_id_seq');
-- ALTER TABLE casbin_rules ALTER COLUMN id SET DEFAULT nextval('casbin_rules_id_seq');

-- End of initialization script