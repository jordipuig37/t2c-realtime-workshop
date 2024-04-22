ALTER ACCOUNT SET TIMEZONE ='Europe/Madrid';

// it's a good practice to use SYSADMIN role to manage snowflake objects rather than ACCOUNTADMIN
USE ROLE SYSADMIN;

// Create the database and two different schemas
CREATE DATABASE REAL_TIME_DEMO;
USE DATABASE REAL_TIME_DEMO;
CREATE SCHEMA INGEST;
CREATE SCHEMA SERVE;
