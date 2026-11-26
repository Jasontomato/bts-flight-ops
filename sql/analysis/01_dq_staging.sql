-- 01_staging.sql
-- Create the staging table and load the standardized Parquet files from S3.
-- Run in Redshift Query Editor v2, connected to the bts-flight-ops workgroup.
--
-- The staging table is a faithful, typed copy of the source: no rules applied.
-- Column order must match the Parquet files exactly, because COPY from Parquet
-- matches columns by position.

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.flights;

CREATE TABLE staging.flights (
    year                             INTEGER,
    month                            INTEGER,
    dayofmonth                       INTEGER,
    dayofweek                        INTEGER,
    flightdate                       DATE,
    reporting_airline                VARCHAR(10),
    dot_id_reporting_airline         INTEGER,
    iata_code_reporting_airline      VARCHAR(10),
    tail_number                      VARCHAR(10),
    flight_number_reporting_airline  INTEGER,
    originairportid                  INTEGER,
    originairportseqid               INTEGER,
    origincitymarketid               INTEGER,
    origin                           VARCHAR(10),
    origincityname                   VARCHAR(100),
    originstate                      VARCHAR(10),
    destairportid                    INTEGER,
    destairportseqid                 INTEGER,
    destcitymarketid                 INTEGER,
    dest                             VARCHAR(10),
    destcityname                     VARCHAR(100),
    deststate                        VARCHAR(10),
    crsdeptime                       VARCHAR(4),
    deptime                          VARCHAR(4),
    depdelay                         DOUBLE PRECISION,
    depdelayminutes                  DOUBLE PRECISION,
    depdel15                         DOUBLE PRECISION,
    taxiout                          DOUBLE PRECISION,
    wheelsoff                        VARCHAR(4),
    wheelson                         VARCHAR(4),
    taxiin                           DOUBLE PRECISION,
    crsarrtime                       VARCHAR(4),
    arrtime                          VARCHAR(4),
    arrdelay                         DOUBLE PRECISION,
    arrdelayminutes                  DOUBLE PRECISION,
    arrdel15                         DOUBLE PRECISION,
    cancelled                        DOUBLE PRECISION,
    cancellationcode                 VARCHAR(10),
    diverted                         DOUBLE PRECISION,
    crselapsedtime                   DOUBLE PRECISION,
    actualelapsedtime                DOUBLE PRECISION,
    airtime                          DOUBLE PRECISION,
    distance                         DOUBLE PRECISION,
    carrierdelay                     DOUBLE PRECISION,
    weatherdelay                     DOUBLE PRECISION,
    nasdelay                         DOUBLE PRECISION,
    securitydelay                    DOUBLE PRECISION,
    lateaircraftdelay                DOUBLE PRECISION,
    divreacheddest                   DOUBLE PRECISION,
    source_file                      VARCHAR(100)
);

-- Load all 12 months. Replace the bucket name with yours.
-- IAM_ROLE default uses the role you set as default on the namespace.
COPY staging.flights
FROM 's3://bts-flight-ops-rj/staging/flights/'
IAM_ROLE default
FORMAT AS PARQUET;

-- Check 1: row counts per month. Compare each with row_count in data/raw/manifest.json.
SELECT year, month, COUNT(*) AS row_count
FROM staging.flights
GROUP BY year, month
ORDER BY year, month;

-- Check 2: one source file per month, and no rows without a source file.
SELECT source_file, COUNT(*) AS row_count
FROM staging.flights
GROUP BY source_file
ORDER BY source_file;

-- Check 3: spot-check that times kept their leading zeros (expect 4 characters).
SELECT LEN(crsdeptime) AS crsdeptime_length, COUNT(*) AS row_count
FROM staging.flights
GROUP BY 1
ORDER BY 1;