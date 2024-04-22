// set a schema just in case, but here we will start to explicitly indicate it
USE SCHEMA INGEST;


// Create a stream on the staging table. This will capture the changing data (CDC)
// of the staging table
CREATE OR REPLACE STREAM
    INGEST.ST_CDC_STG_F_BICING_STATIONS
ON TABLE INGEST.STG_F_BICING_STATIONS
;

// we should expect false the first time, but after some time we will see true
select system$stream_has_data('ST_CDC_STG_F_BICING_STATIONS');

USE SCHEMA SERVE;

// Create a table to store the last updated status
CREATE OR REPLACE TABLE
    SERVE.F_LAST_UPDATED_STATUS
(
    STATION_ID INTEGER,
    NUM_BIKES_AVAILABLE INTEGER,
    NUM_BIKES_AVAILABLE_MECHANICAL INTEGER,
    NUM_BIKES_AVAILABLE_EBIKE INTEGER,
    NUM_DOCKS_AVAILABLE INTEGER,
    LAST_REPORTED INTEGER,
    LAST_REPORTED_TST TIMESTAMP_NTZ(9),
    IS_CHARGING_STATION BOOLEAN,
    STATUS VARCHAR(1024),
    IS_INSTALLED INTEGER,
    IS_RENTING INTEGER,
    IS_RETURNING INTEGER,
    TRAFFIC VARCHAR(1024),
    TST_REC TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE PROCEDURE
    SERVE.SP_REFRESH_LAST_UPDATED_STATUS()
/** This is the stored procedure that executes the logic. */
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    TRUNCATE TABLE SERVE.F_LAST_UPDATED_STATUS;
    INSERT INTO SERVE.F_LAST_UPDATED_STATUS
    SELECT
        STATION_ID,
        NUM_BIKES_AVAILABLE,
        NUM_BIKES_AVAILABLE_MECHANICAL,
        NUM_BIKES_AVAILABLE_EBIKE,
        NUM_DOCKS_AVAILABLE,
        LAST_REPORTED,
        to_timestamp(LAST_REPORTED) AS LAST_REPORTED_TST,
        IS_CHARGING_STATION,
        STATUS,
        IS_INSTALLED,
        IS_RENTING,
        IS_RETURNING,
        TRAFFIC,
        TST_REC
    FROM INGEST.ST_CDC_STG_F_BICING_STATIONS;
    RETURN 'Done';
END
$$
;

CREATE OR REPLACE TASK
    SERVE.TSK_REFRESH_LAST_UPDATED_STATUS
    WAREHOUSE='COMPUTE_WH'
    SCHEDULE = '5 minutes'  // we use this option
WHEN
    system$stream_has_data('ST_CDC_STG_F_BICING_STATIONS')
AS
    CALL SERVE.SP_REFRESH_LAST_UPDATED_STATUS()
;

ALTER TASK SERVE.TSK_REFRESH_LAST_UPDATED_STATUS RESUME;

-------------------------------------------------------------------------------

// Create a table with the historic of the status. Here we will aggregate the
// data in buckets of 20 minutes and add the master data information
CREATE OR REPLACE TABLE
    SERVE.F_HISTORIC_STATION_STATUS
(
    STATION_ID INTEGER,
    TIME_WINDOW_START TIMESTAMP_NTZ(9),
    STATION_NAME VARCHAR(1024),
    LAT NUMBER(12, 7),
    LON NUMBER(12, 7),
    ADDRESS VARCHAR(1024),
    CAPACITY INTEGER,
    --- fact data:
    NUM_BIKES_AVAILABLE INTEGER,
    NUM_BIKES_AVAILABLE_MECHANICAL INTEGER,
    NUM_BIKES_AVAILABLE_EBIKE INTEGER,
    NUM_DOCKS_AVAILABLE INTEGER,
    NUM_BIKES_IN INTEGER,
    NUM_BIKES_OUT INTEGER,
    STATUS VARCHAR(1024),
    TST_REC TIMESTAMP_NTZ(9)
);

// create a stored procedure that will process the data and load this table and
// a task that will be executed every 20 minutes
CREATE OR REPLACE PROCEDURE
    SP_COMPUTE_WINDOW_STATION_STATUS()
/** This is the stored procedure that loads F_HISTORIC_STATION_STATUS
    by aggregating data from staging and enriching it with station master data
*/
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    // create a temp table to store the data using a CTAS statement and CTE
    CREATE TEMPORARY TABLE
        TEMP_F_WINDOW_STATION_STATUS
    AS
    WITH
    stations_master AS (
        SELECT
            STATION_ID,
            STATION_NAME,
            LAT, LON,
            ADDRESS,
            CAPACITY
        FROM M_BICING_STATIONS
    ),

    last_20_min_data AS (
        SELECT
            STATION_ID,
            NUM_BIKES_AVAILABLE,
            NUM_BIKES_AVAILABLE - LAG(NUM_BIKES_AVAILABLE, 1) OVER (
                PARTITION BY STATION_ID
                ORDER BY LAST_REPORTED) AS BIKES_DIFF,
            NUM_BIKES_AVAILABLE_MECHANICAL,
            NUM_BIKES_AVAILABLE_EBIKE,
            NUM_DOCKS_AVAILABLE,
            STATUS,
            to_timestamp(LAST_REPORTED) AS LAST_REPORTED_TST
        FROM INGEST.STG_F_BICING_STATIONS_STATUS
        WHERE to_timestamp(LAST_REPORTED) >= DATEADD(MINUTE, -20, current_timestamp())
    ),

    aggregated_20_min AS (
        SELECT
            STATION_ID,
            MIN(LAST_REPORTED_TST) AS TIME_WINDOW_START,
            MEDIAN(NUM_BIKES_AVAILABLE)
            MEDIAN(NUM_BIKES_AVAILABLE_MECHANICAL)
            MEDIAN(NUM_BIKES_AVAILABLE_EBIKE)
            MEDIAN(NUM_DOCKS_AVAILABLE)
            SUM(CASE BIKES_DIFF > 0 THEN BIKES_DIFF ELSE 0) AS BIKES_IN
            SUM(CASE BIKES_DIFF < 0 THEN BIKES_DIFF ELSE 0) AS BIKES_OUT

        FROM last_20_min_data
        GROUP BY STATION_ID
    ),

    final AS (
        SELECT
            agr.STATION_ID,
            agr.TIME_WINDOW_START,
            mm.STATION_NAME,
            mm.LAT, mm.LON,
            mm.ADDRESS, mm.CAPACITY,
            agr.NUM_BIKES_AVAILABLE,
            NUM_BIKES_AVAILABLE_MECHANICAL,
            NUM_BIKES_AVAILABLE_EBIKE,
            NUM_DOCKS_AVAILABLE,
            NUM_BIKES_IN,
            NUM_BIKES_OUT,
            STATUS,
            current_timestamp() AS TST_REC
        FROM aggregated_20_min agr
        LEFT JOIN stations_master mm ON
            agr.STATION_ID = mm.STATION_ID
    )

    SELECT * FROM final;

    // perform the insert statement
    INSERT INTO F_HISTORIC_STATION_STATUS
    SELECT
        *
    FROM TEMP_F_WINDOW_STATION_STATUS;

    RETURN 'Done';
END
$$
;

// and create the task that will run every 20 minutes
CREATE OR REPLACE TASK
    TSK_COMPUTE_WINDOW_STATION_STATUS
    WAREHOUSE='COMPUTE_WH'
    SCHEDULE = '20 minutes'
AS
    CALL SP_COMPUTE_WINDOW_STATION_STATUS()
;

ALTER TASK TSK_COMPUTE_WINDOW_STATION_STATUS RESUME;
