USE SCHEMA SERVE;

CREATE OR REPLACE VIEW
    V_STATIONS_DISTANCE_MATRIX
AS;
WITH
m_stations AS (
    SELECT
        STATION_ID,
        LAT, LON
    FROM M_BICING_STATIONS
),

auto_joined AS (
    SELECT
        t1.STATION_ID AS SRC,
        t2.STATION_ID AS TGT,
        st_distance(st_makepoint(t1.LON, t1.LAT), st_makepoint(t2.LON, t2.LAT)) AS DISTANCE
    FROM m_stations t1 FULL OUTER JOIN m_stations t2
)

// [ ] PIVOT IT TO MAKE IT A MATRIX

SELECT *
FROM auto_joined;

SELECT * 
  FROM monthly_sales
    PIVOT(SUM(amount) FOR MONTH IN ('JAN', 'FEB', 'MAR', 'APR'))
      AS p
  ORDER BY EMPID;
+-------+-------+-------+-------+-------+
| EMPID | 'JAN' | 'FEB' | 'MAR' | 'APR' |
|-------+-------+-------+-------+-------|
|     1 | 10400 |  8000 | 11000 | 18000 |
|     2 | 39500 | 90700 | 12000 |  5300 |
+-------+-------+-------+-------+-------+