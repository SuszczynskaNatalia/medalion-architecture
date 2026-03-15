CREATE OR REPLACE DATABASE TAXI_PROJECT;

USE DATABASE TAXI_PROJECT;

-- -----------------------------------------------------
-- Create schemas for Medallion Architecture
-- -----------------------------------------------------

-- Bronze layer: raw ingested data
CREATE OR REPLACE SCHEMA BRONZE;

-- Silver layer: cleaned and standardized data
CREATE OR REPLACE SCHEMA SILVER;

-- Gold layer: curated and aggregated data for analytics
CREATE OR REPLACE SCHEMA GOLD;