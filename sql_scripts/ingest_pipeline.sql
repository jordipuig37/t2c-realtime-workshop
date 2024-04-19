USE SCHEMA INGEST;

// Create the staging that points to the location where we will read data from.
CREATE OR REPLACE STAGE S_T2C_REALTIME_DATA 
	URL = 'azure://t2crealtimeworkshop.blob.core.windows.net/bicing' 
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'Link to the public container of bicing data.';

// create a json file format to add STRIP_OUTER_ARRAY = TRUE option
CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

// Next create the table where our data will land
CREATE OR REPLACE TABLE
    STG_F_BICING_STATIONS_STATUS
(
    STATION_ID INTEGER,
    NUM_BIKES_AVAILABLE INTEGER,
    NUM_BIKES_AVAILABLE_MECHANICAL INTEGER,
    NUM_BIKES_AVAILABLE_EBIKE INTEGER,
    NUM_DOCKS_AVAILABLE INTEGER,
    LAST_REPORTED INTEGER,
    IS_CHARGING_STATION BOOLEAN,
    STATUS VARCHAR(1024),
    IS_INSTALLED INTEGER,
    IS_RENTING INTEGER,
    IS_RETURNING INTEGER,
    TRAFFIC VARCHAR(1024),
    TST_REC TIMESTAMP_NTZ(9)
);


// let's test the following COPY INTO statement
COPY INTO STG_F_BICING_STATIONS_STATUS
FROM (
    SELECT
        get($1, 'station_id')::number AS STATION_ID,
        get($1, 'num_bikes_available')::number AS NUM_BIKES_AVAILABLE,
        get(get($1, 'num_bikes_available_types'), 'mechanical')::number AS NUM_BIKES_AVAILABLE_MECHANICAL,
        get(get($1, 'num_bikes_available_types'), 'ebike')::number NUM_BIKES_AVAILABLE_EBIKE,
        get($1, 'num_docks_available')::number NUM_DOCKS_AVAILABLE,
        get($1, 'last_reported')::number AS LAST_REPORTED,
        get($1, 'is_charging_station')::boolean AS IS_CHARGING_STATION,
        get($1, 'status')::text AS STATUS,
        get($1, 'is_installed')::number AS IS_INSTALLED,
        get($1, 'is_renting')::number AS IS_RENTING,
        get($1, 'is_returning')::number AS IS_RETURNING,
        get($1, 'traffic')::text AS TRAFFIC,
        current_timestamp() AS TST_REC

    FROM @S_T2C_REALTIME_DATA/daily_data/stations
)
FILE_FORMAT = json_format
;

// TODOs
-------------------------------------------------------------------------------
// [ ] Create integration
// [ ] Provision event queue on azure side
CREATE OR REPLACE PIPE
    P_BICING_STATIONS
AS
COPY INTO STG_F_BICINT_STATIONS
FROM @S_T2C_REALTIME_DATA/daily_data
FILE_FORMAT = json_format
;
-------------------------------------------------------------------------------

// Load the master data table which contains data for each station
// We will load the data directly to the serving schema because this will data
// is static.
// [ ] TODO

USE SCHEMA SERVE;

CREATE OR REPLACE TABLE
    M_BICING_STATIONS
(
    STATION_ID INTEGER,
    NAME VARCHAR(1024),
    PHYSICAL_CONFIGURATION VARCHAR(64),
    LAT NUMBER(12, 7),
    LON NUMBER(12, 7),
    ALTITUDE NUMBER(12, 2),
    ADDRESS VARCHAR(1024),
    POST_CODE VARCHAR(5),
    CAPACITY INTEGER,
    IS_CHARGING_STATION BOOLEAN,
    NEARBY_DISTANCE NUMBER(8, 2),
    _RIDE_CODE_SUPPORT BOOLEAN,
    RENTAL_URIS VARCHAR(1024),
    TST_REC TIMESTAMP_NTZ(9)
);

truncate table M_BICING_STATIONS;
COPY INTO M_BICING_STATIONS
FROM (
    SELECT
        get($1, 'station_id')::number AS STATION_ID,
        get($1, 'name')::text AS NAME,
        get($1, 'physical_configuration')::text AS PHYSICAL_CONFIGURATION,
        get($1, 'lat')::decimal AS LAT,
        get($1, 'lon')::decimal AS LON,
        get($1, 'altitude')::number AS ALTITUDE,
        get($1, 'address')::text AS ADDRESS,
        get($1, 'post_code')::text AS POST_CODE,
        get($1, 'capacity')::number AS CAPACITY,
        get($1, 'is_charging_station')::boolean AS IS_CHARGING_STATION,
        get($1, 'nearby_distance')::number AS NEARBY_DISTANCE,
        get($1, '_ride_code_support')::boolean AS _RIDE_CODE_SUPPORT,
        get($1, 'rental_uris')::text AS RENTAL_URIS,
        current_timestamp() AS TST_REC

    FROM @INGEST.S_T2C_REALTIME_DATA/master/stations
)
FILE_FORMAT = INGEST.json_format
;

select * from M_BICING_STATIONS;

SELECT
        get($1, 'station_id')::number AS STATION_ID,
        get($1, 'name')::text AS NAME,
        get($1, 'physical_configuration')::text AS PHYSICAL_CONFIGURATION,
        to_decimal(get($1, 'lat')::text, 12, 7) AS LAT,
        to_decimal(get($1, 'lon')::text, 12, 7) AS LON,
        get($1, 'altitude')::number AS ALTITUDE,
        get($1, 'address')::text AS ADDRESS,
        get($1, 'post_code')::text AS POST_CODE,
        get($1, 'capacity')::number AS CAPACITY,
        get($1, 'is_charging_station')::boolean AS IS_CHARGING_STATION,
        get($1, 'nearby_distance')::number AS NEARBY_DISTANCE,
        get($1, '_ride_code_support')::boolean AS _RIDE_CODE_SUPPORT,
        get($1, 'rental_uris')::text AS RENTAL_URIS,
        current_timestamp() AS TST_REC

    FROM @INGEST.S_T2C_REALTIME_DATA/master/stations
(FILE_FORMAT => 'INGEST.json_format')
