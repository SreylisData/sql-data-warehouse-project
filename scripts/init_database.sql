/*
=============================================================
Create Database
=============================================================
Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists. 
If the database exists, it is dropped and recreated. Additionally, the script sets up three 
schemas within the database: 'bronze', 'silver', 'gold'.	

WARNING:
Running this script will drop the entire 'DataWarehouse' database if it exists. 
All data in the database will be permanently deleted. Proceed with caution 
and ensure you have proper backups before running this script.
*/
-- Drop and recreate the 'DataWarehouse' database
DROP DATABASE IF EXISTS DATAWAREHOUSE;

-- Create the 'DataWarehouse' database
CREATE DATABASE DATAWAREHOUSE;

USE DATAWAREHOUSE;

-- Create Schemas
CREATE SCHEMA BRONZE;

CREATE SCHEMA SILVER;

CREATE SCHEMA GOLD;
