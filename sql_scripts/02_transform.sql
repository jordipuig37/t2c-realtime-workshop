// set a schema just in case, but here we will start to explicitly indicate it
USE SCHEMA SERVE;


// Create a stream on the staging table. This will capture the changing data (CDC)
// of the staging table
CREATE OR REPLACE STREAM
    SERVE.ST_CDC_STG_F_BICING_STATIONS
ON TABLE INGEST.STG_F_BICING_STATIONS_STATUS
;

// we should expect false the first time, but after some time we will see true
select system$stream_has_data('ST_CDC_STG_F_BICING_STATIONS');


// Create a table to store the last updated status
CREATE OR REPLACE TABLE
    SERVE.F_LAST_UPDATED_STATUS
(
    STATION_ID INTEGER,
    STATION_NAME VARCHAR(1024),
    LAT NUMBER(12, 7),
    LON NUMBER(12, 7),
    CAPACITY INTEGER,
    NUM_BIKES_AVAILABLE INTEGER,
    NUM_BIKES_AVAILABLE_MECHANICAL INTEGER,
    NUM_BIKES_AVAILABLE_EBIKE INTEGER,
    NUM_DOCKS_AVAILABLE INTEGER,
    OCCUPATION_RATIO NUMBER(12,4),
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
    CREATE OR REPLACE TEMPORARY TABLE
        TEMP_F_LAST_UPDATED_STATUS_TO_MERGE
    AS
    WITH
    m_stations AS (
        SELECT STATION_ID, STATION_NAME, LAT, LON, CAPACITY
        FROM SERVE.M_BICING_STATIONS
    ),

    new_status AS (
        SELECT *
        FROM SERVE.ST_CDC_STG_F_BICING_STATIONS
    ),

    final AS (
        SELECT
            ff.STATION_ID,
            mm.STATION_NAME,
            mm.LAT,
            mm.LON,
            mm.CAPACITY,
            ff.NUM_BIKES_AVAILABLE,
            ff.NUM_BIKES_AVAILABLE_MECHANICAL,
            ff.NUM_BIKES_AVAILABLE_EBIKE,
            ff.NUM_DOCKS_AVAILABLE,
            ff.NUM_BIKES_AVAILABLE / mm.CAPACITY AS OCCUPATION_RATIO,
            ff.LAST_REPORTED,
            to_timestamp(ff.LAST_REPORTED) AS LAST_REPORTED_TST,
            ff.IS_CHARGING_STATION,
            ff.STATUS,
            ff.IS_INSTALLED,
            ff.IS_RENTING,
            ff.IS_RETURNING,
            ff.TRAFFIC,
            CURRENT_TIMESTAMP() AS TST_REC
        FROM new_status ff
        LEFT JOIN m_stations mm ON
            ff.STATION_ID = mm.STATION_ID
    )

    SELECT * FROM final;

    MERGE INTO SERVE.F_LAST_UPDATED_STATUS AS tgt
    USING TEMP_F_LAST_UPDATED_STATUS_TO_MERGE AS src
    ON tgt.STATION_ID = src.STATION_ID
    WHEN MATCHED THEN
        UPDATE SET
            tgt.STATION_NAME = src.STATION_NAME,
            tgt.LAT = src.LAT,
            tgt.LON = src.LON,
            tgt.CAPACITY = src.CAPACITY,
            tgt.NUM_BIKES_AVAILABLE = src.NUM_BIKES_AVAILABLE,
            tgt.NUM_BIKES_AVAILABLE_MECHANICAL = src.NUM_BIKES_AVAILABLE_MECHANICAL,
            tgt.NUM_BIKES_AVAILABLE_EBIKE = src.NUM_BIKES_AVAILABLE_EBIKE,
            tgt.NUM_DOCKS_AVAILABLE = src.NUM_DOCKS_AVAILABLE,
            tgt.OCCUPATION_RATIO = src.OCCUPATION_RATIO,
            tgt.LAST_REPORTED = src.LAST_REPORTED,
            tgt.LAST_REPORTED_TST = src.LAST_REPORTED_TST,
            tgt.IS_CHARGING_STATION = src.IS_CHARGING_STATION,
            tgt.STATUS = src.STATUS,
            tgt.IS_INSTALLED = src.IS_INSTALLED,
            tgt.IS_RENTING = src.IS_RENTING,
            tgt.IS_RETURNING = src.IS_RETURNING,
            tgt.TRAFFIC = src.TRAFFIC,
            tgt.TST_REC = src.TST_REC
    WHEN NOT MATCHED THEN
    INSERT (
        STATION_ID,
        STATION_NAME,
        LAT,
        LON,
        CAPACITY,
        NUM_BIKES_AVAILABLE,
        NUM_BIKES_AVAILABLE_MECHANICAL,
        NUM_BIKES_AVAILABLE_EBIKE,
        NUM_DOCKS_AVAILABLE,
        OCCUPATION_RATIO,
        LAST_REPORTED,
        LAST_REPORTED_TST,
        IS_CHARGING_STATION,
        STATUS,
        IS_INSTALLED,
        IS_RENTING,
        IS_RETURNING,
        TRAFFIC,
        TST_REC
    )
    VALUES (
        src.STATION_ID,
        src.STATION_NAME,
        src.LAT,
        src.LON,
        src.CAPACITY,
        src.NUM_BIKES_AVAILABLE,
        src.NUM_BIKES_AVAILABLE_MECHANICAL,
        src.NUM_BIKES_AVAILABLE_EBIKE,
        src.NUM_DOCKS_AVAILABLE,
        src.OCCUPATION_RATIO,
        src.LAST_REPORTED,
        src.LAST_REPORTED_TST,
        src.IS_CHARGING_STATION,
        src.STATUS,
        src.IS_INSTALLED,
        src.IS_RENTING,
        src.IS_RETURNING,
        src.TRAFFIC,
        src.TST_REC
    );
    RETURN 'Processed ' || (select count(*) from TEMP_F_LAST_UPDATED_STATUS_TO_MERGE) || ' rows';
END
$$
;

call SERVE.SP_REFRESH_LAST_UPDATED_STATUS();

SELECT * FROM F_LAST_UPDATED_STATUS;

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
