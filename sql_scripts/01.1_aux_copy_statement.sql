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

    FROM @S_BICING_RAW_DATA/stations  // use the stage
)
FILE_FORMAT = json_format  // use the file format
;
