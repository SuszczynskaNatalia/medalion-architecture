CREATE DATABASE IF NOT EXISTS TAXI_PROJECT;

USE DATABASE TAXI_PROJECT;

-- -----------------------------------------------------
-- Create schemas for Medallion Architecture
-- -----------------------------------------------------

-- Bronze layer: raw ingested data
CREATE SCHEMA IF NOT EXISTS BRONZE;

-- Silver layer: cleaned and standardized data
CREATE SCHEMA IF NOT EXISTS SILVER;

-- Gold layer: curated and aggregated data for analytics
CREATE SCHEMA IF NOT EXISTS GOLD;