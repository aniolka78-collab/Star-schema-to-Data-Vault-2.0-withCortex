-- =============================================================================
-- DELIVERY NETWORK DATA WAREHOUSE - STAR SCHEMA
-- Database: PL_AKHRAMKO_DB  |  Schema: DELIVERY_DW
-- 7 Dimension Tables + 1 Fact Table  |  ~1M shipments, ~10K addresses
-- =============================================================================

USE WAREHOUSE CORTEX_CODE_EMEA_WH;

CREATE OR REPLACE SCHEMA PL_AKHRAMKO_DB.DELIVERY_DW;

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- DIM_DATE: Calendar dimension (2023-2025)
CREATE OR REPLACE TABLE PL_AKHRAMKO_DB.DELIVERY_DW.DIM_DATE (
    DATE_KEY        INT          NOT NULL PRIMARY KEY,
    FULL_DATE       DATE         NOT NULL,
    DAY_OF_WEEK     TINYINT      NOT NULL,
    DAY_NAME        VARCHAR(10)  NOT NULL,
    DAY_OF_MONTH    TINYINT      NOT NULL,
    DAY_OF_YEAR     SMALLINT     NOT NULL,
    WEEK_OF_YEAR    TINYINT      NOT NULL,
    MONTH_NUMBER    TINYINT      NOT NULL,
    MONTH_NAME      VARCHAR(10)  NOT NULL,
    QUARTER         TINYINT      NOT NULL,
    YEAR            SMALLINT     NOT NULL,
    IS_WEEKEND      BOOLEAN      NOT NULL,
    IS_HOLIDAY      BOOLEAN      NOT NULL
);

-- DIM_ADDRESS: Origin and destination addresses (~10K rows)
CREATE OR REPLACE TABLE PL_AKHRAMKO_DB.DELIVERY_DW.DIM_ADDRESS (
    ADDRESS_KEY     INT          NOT NULL PRIMARY KEY,
    STREET_LINE1    VARCHAR(200) NOT NULL,
    STREET_LINE2    VARCHAR(200),
    CITY            VARCHAR(100) NOT NULL,
    STATE_PROVINCE  VARCHAR(50)  NOT NULL,
    POSTAL_CODE     VARCHAR(20)  NOT NULL,
    COUNTRY         VARCHAR(50)  NOT NULL,
    LATITUDE        FLOAT,
    LONGITUDE       FLOAT,
    ADDRESS_TYPE    VARCHAR(20)  NOT NULL,
    REGION          VARCHAR(50)  NOT NULL
);

-- DIM_CUSTOMER: Senders and receivers (~5K rows)
CREATE OR REPLACE TABLE PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CUSTOMER (
    CUSTOMER_KEY      INT          NOT NULL PRIMARY KEY,
    CUSTOMER_ID       VARCHAR(20)  NOT NULL,
    FIRST_NAME        VARCHAR(50)  NOT NULL,
    LAST_NAME         VARCHAR(50)  NOT NULL,
    COMPANY_NAME      VARCHAR(100),
    CUSTOMER_TYPE     VARCHAR(20)  NOT NULL,
    EMAIL             VARCHAR(100),
    PHONE             VARCHAR(20),
    REGISTRATION_DATE DATE         NOT NULL,
    LOYALTY_TIER      VARCHAR(20)  NOT NULL
);

-- DIM_CARRIER: Delivery carriers (10 rows)
CREATE OR REPLACE TABLE PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CARRIER (
    CARRIER_KEY       INT          NOT NULL PRIMARY KEY,
    CARRIER_CODE      VARCHAR(10)  NOT NULL,
    CARRIER_NAME      VARCHAR(100) NOT NULL,
    CARRIER_TYPE      VARCHAR(30)  NOT NULL,
    HEADQUARTERS      VARCHAR(100) NOT NULL,
    FLEET_SIZE        INT          NOT NULL,
    RATING            FLOAT        NOT NULL
);

-- DIM_SERVICE_TYPE: Shipping service levels (10 rows)
CREATE OR REPLACE TABLE PL_AKHRAMKO_DB.DELIVERY_DW.DIM_SERVICE_TYPE (
    SERVICE_TYPE_KEY  INT          NOT NULL PRIMARY KEY,
    SERVICE_CODE      VARCHAR(10)  NOT NULL,
    SERVICE_NAME      VARCHAR(50)  NOT NULL,
    DELIVERY_SPEED    VARCHAR(20)  NOT NULL,
    MAX_WEIGHT_KG     FLOAT        NOT NULL,
    MAX_LENGTH_CM     FLOAT        NOT NULL,
    TRACKING_INCLUDED BOOLEAN      NOT NULL,
    INSURANCE_INCLUDED BOOLEAN     NOT NULL,
    SIGNATURE_REQUIRED BOOLEAN     NOT NULL
);

-- DIM_PACKAGE_TYPE: Package categories (10 rows)
CREATE OR REPLACE TABLE PL_AKHRAMKO_DB.DELIVERY_DW.DIM_PACKAGE_TYPE (
    PACKAGE_TYPE_KEY  INT          NOT NULL PRIMARY KEY,
    PACKAGE_CODE      VARCHAR(10)  NOT NULL,
    PACKAGE_NAME      VARCHAR(50)  NOT NULL,
    CATEGORY          VARCHAR(20)  NOT NULL,
    MAX_WEIGHT_KG     FLOAT        NOT NULL,
    MAX_DIMENSIONS_CM VARCHAR(30)  NOT NULL,
    IS_FRAGILE        BOOLEAN      NOT NULL,
    REQUIRES_COOLING  BOOLEAN      NOT NULL
);

-- DIM_STATUS: Shipment lifecycle statuses (12 rows)
CREATE OR REPLACE TABLE PL_AKHRAMKO_DB.DELIVERY_DW.DIM_STATUS (
    STATUS_KEY        INT          NOT NULL PRIMARY KEY,
    STATUS_CODE       VARCHAR(10)  NOT NULL,
    STATUS_NAME       VARCHAR(50)  NOT NULL,
    STATUS_CATEGORY   VARCHAR(30)  NOT NULL,
    IS_TERMINAL       BOOLEAN      NOT NULL,
    DISPLAY_ORDER     INT          NOT NULL
);

-- =============================================================================
-- FACT TABLE
-- =============================================================================

CREATE OR REPLACE TABLE PL_AKHRAMKO_DB.DELIVERY_DW.FACT_SHIPMENTS (
    SHIPMENT_KEY          INT          NOT NULL PRIMARY KEY,
    TRACKING_NUMBER       VARCHAR(30)  NOT NULL,
    SHIP_DATE_KEY         INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_DATE(DATE_KEY),
    DELIVERY_DATE_KEY     INT          REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_DATE(DATE_KEY),
    ORIGIN_ADDRESS_KEY    INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_ADDRESS(ADDRESS_KEY),
    DEST_ADDRESS_KEY      INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_ADDRESS(ADDRESS_KEY),
    SENDER_CUSTOMER_KEY   INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CUSTOMER(CUSTOMER_KEY),
    RECEIVER_CUSTOMER_KEY INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CUSTOMER(CUSTOMER_KEY),
    CARRIER_KEY           INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CARRIER(CARRIER_KEY),
    SERVICE_TYPE_KEY      INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_SERVICE_TYPE(SERVICE_TYPE_KEY),
    PACKAGE_TYPE_KEY      INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_PACKAGE_TYPE(PACKAGE_TYPE_KEY),
    STATUS_KEY            INT          NOT NULL REFERENCES PL_AKHRAMKO_DB.DELIVERY_DW.DIM_STATUS(STATUS_KEY),
    WEIGHT_KG             FLOAT        NOT NULL,
    DECLARED_VALUE_USD    FLOAT,
    SHIPPING_COST_USD     FLOAT        NOT NULL,
    INSURANCE_COST_USD    FLOAT        NOT NULL DEFAULT 0,
    DISTANCE_KM           FLOAT        NOT NULL,
    TRANSIT_DAYS_PLANNED  INT          NOT NULL,
    TRANSIT_DAYS_ACTUAL   INT,
    DELIVERY_ATTEMPTS     INT          NOT NULL DEFAULT 1,
    IS_RETURN             BOOLEAN      NOT NULL DEFAULT FALSE,
    IS_DAMAGED            BOOLEAN      NOT NULL DEFAULT FALSE,
    IS_LOST               BOOLEAN      NOT NULL DEFAULT FALSE
);

-- =============================================================================
-- POPULATE DIMENSION TABLES
-- =============================================================================

-- DIM_DATE: 3 years of calendar data
INSERT INTO PL_AKHRAMKO_DB.DELIVERY_DW.DIM_DATE
SELECT
    TO_NUMBER(TO_CHAR(d.FULL_DATE, 'YYYYMMDD')) AS DATE_KEY,
    d.FULL_DATE,
    DAYOFWEEKISO(d.FULL_DATE) AS DAY_OF_WEEK,
    DAYNAME(d.FULL_DATE) AS DAY_NAME,
    DAY(d.FULL_DATE) AS DAY_OF_MONTH,
    DAYOFYEAR(d.FULL_DATE) AS DAY_OF_YEAR,
    WEEKOFYEAR(d.FULL_DATE) AS WEEK_OF_YEAR,
    MONTH(d.FULL_DATE) AS MONTH_NUMBER,
    MONTHNAME(d.FULL_DATE) AS MONTH_NAME,
    QUARTER(d.FULL_DATE) AS QUARTER,
    YEAR(d.FULL_DATE) AS YEAR,
    CASE WHEN DAYOFWEEKISO(d.FULL_DATE) IN (6, 7) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
    FALSE AS IS_HOLIDAY
FROM (
    SELECT DATEADD(DAY, SEQ4(), '2023-01-01'::DATE) AS FULL_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 1096))
) d
WHERE d.FULL_DATE <= '2025-12-31';

-- DIM_STATUS
INSERT INTO PL_AKHRAMKO_DB.DELIVERY_DW.DIM_STATUS VALUES
(1, 'CREATED',   'Order Created',         'Pre-Transit', FALSE, 1),
(2, 'PICKUP',    'Picked Up',             'Pre-Transit', FALSE, 2),
(3, 'SORTED',    'At Sorting Facility',   'In-Transit',  FALSE, 3),
(4, 'INTRANSIT', 'In Transit',            'In-Transit',  FALSE, 4),
(5, 'OUTHUB',    'Out for Delivery',      'In-Transit',  FALSE, 5),
(6, 'DELIVERY',  'Delivered',             'Completed',   TRUE,  6),
(7, 'FAILED',    'Delivery Failed',       'Exception',   FALSE, 7),
(8, 'RETURNED',  'Returned to Sender',    'Completed',   TRUE,  8),
(9, 'LOST',      'Lost',                  'Exception',   TRUE,  9),
(10,'DAMAGED',   'Damaged in Transit',    'Exception',   FALSE, 10),
(11,'HELD',      'Held at Customs',       'Exception',   FALSE, 11),
(12,'REDIRECT',  'Redirected',            'In-Transit',  FALSE, 12);

-- DIM_CARRIER
INSERT INTO PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CARRIER VALUES
(1,  'FEDX',  'FedEx',              'National',      'Memphis, TN',       45000, 4.5),
(2,  'UPS',   'UPS',                'National',      'Atlanta, GA',       55000, 4.4),
(3,  'USPS',  'US Postal Service',  'Government',    'Washington, DC',    230000, 4.0),
(4,  'DHL',   'DHL Express',        'International', 'Bonn, Germany',     35000, 4.3),
(5,  'AMZN',  'Amazon Logistics',   'E-Commerce',    'Seattle, WA',       60000, 4.2),
(6,  'ONTRC', 'OnTrac',             'Regional',      'Chandler, AZ',      3500,  3.8),
(7,  'LSO',   'Lone Star Overnight','Regional',      'Austin, TX',        1200,  3.9),
(8,  'SPED',  'Spee-Dee Delivery',  'Regional',      'St. Cloud, MN',     800,   4.1),
(9,  'PUROL', 'Purolator',          'International', 'Mississauga, Canada',12000, 4.0),
(10, 'ROYAL', 'Royal Mail',         'International', 'London, UK',        150000, 3.7);

-- DIM_SERVICE_TYPE
INSERT INTO PL_AKHRAMKO_DB.DELIVERY_DW.DIM_SERVICE_TYPE VALUES
(1, 'STD',    'Standard Ground',       'Standard',    30.0,  150.0, TRUE,  FALSE, FALSE),
(2, 'EXP',    'Express',               'Express',     25.0,  120.0, TRUE,  FALSE, FALSE),
(3, 'OVNT',   'Overnight',             'Overnight',   20.0,  100.0, TRUE,  TRUE,  TRUE),
(4, 'SAME',   'Same Day',              'Same-Day',    10.0,  80.0,  TRUE,  TRUE,  TRUE),
(5, 'ECON',   'Economy',               'Economy',     35.0,  200.0, FALSE, FALSE, FALSE),
(6, 'PRIOR',  'Priority Mail',         'Priority',    30.0,  120.0, TRUE,  TRUE,  FALSE),
(7, 'FRGT',   'Freight',               'Freight',     500.0, 300.0, TRUE,  TRUE,  TRUE),
(8, 'INTL',   'International Standard','International',25.0, 150.0, TRUE,  FALSE, FALSE),
(9, 'LTTR',   'Letter Service',        'Letter',      0.5,   35.0,  FALSE, FALSE, FALSE),
(10,'CERT',   'Certified Mail',        'Certified',   2.0,   40.0,  TRUE,  FALSE, TRUE);

-- DIM_PACKAGE_TYPE
INSERT INTO PL_AKHRAMKO_DB.DELIVERY_DW.DIM_PACKAGE_TYPE VALUES
(1,  'ENV',   'Standard Envelope',   'Letter',   0.5,  '35x25x1',     FALSE, FALSE),
(2,  'LENV',  'Large Envelope',      'Letter',   1.0,  '40x30x3',     FALSE, FALSE),
(3,  'SBOX',  'Small Box',           'Parcel',   5.0,  '30x20x15',    FALSE, FALSE),
(4,  'MBOX',  'Medium Box',          'Parcel',   15.0, '45x35x25',    FALSE, FALSE),
(5,  'LBOX',  'Large Box',           'Parcel',   30.0, '60x45x35',    FALSE, FALSE),
(6,  'TUBE',  'Tube',                'Special',  5.0,  '100x15x15',   FALSE, FALSE),
(7,  'FRAG',  'Fragile Package',     'Special',  20.0, '50x40x30',    TRUE,  FALSE),
(8,  'COLD',  'Insulated Container', 'Special',  25.0, '50x40x40',    FALSE, TRUE),
(9,  'PALL',  'Pallet',              'Freight',  500.0,'120x100x150', FALSE, FALSE),
(10, 'FLAT',  'Flat Rate Box',       'Parcel',   30.0, '35x30x15',    FALSE, FALSE);

-- DIM_ADDRESS: 10,000 synthetic US addresses across 30 cities
INSERT INTO PL_AKHRAMKO_DB.DELIVERY_DW.DIM_ADDRESS
WITH cities AS (
    SELECT column1 AS CITY, column2 AS STATE_PROVINCE, column3 AS BASE_LAT, column4 AS BASE_LON, column5 AS REGION,
           ROW_NUMBER() OVER (ORDER BY column1) - 1 AS CITY_IDX
    FROM VALUES
        ('New York','NY',40.71,-74.01,'Northeast'),('Los Angeles','CA',34.05,-118.24,'West'),
        ('Chicago','IL',41.88,-87.63,'Midwest'),('Houston','TX',29.76,-95.37,'South'),
        ('Phoenix','AZ',33.45,-112.07,'West'),('Philadelphia','PA',39.95,-75.17,'Northeast'),
        ('San Antonio','TX',29.42,-98.49,'South'),('San Diego','CA',32.72,-117.16,'West'),
        ('Dallas','TX',32.78,-96.80,'South'),('San Jose','CA',37.34,-121.89,'West'),
        ('Austin','TX',30.27,-97.74,'South'),('Jacksonville','FL',30.33,-81.66,'South'),
        ('Fort Worth','TX',32.76,-97.33,'South'),('Columbus','OH',39.96,-83.00,'Midwest'),
        ('Charlotte','NC',35.23,-80.84,'South'),('Indianapolis','IN',39.77,-86.16,'Midwest'),
        ('San Francisco','CA',37.77,-122.42,'West'),('Seattle','WA',47.61,-122.33,'West'),
        ('Denver','CO',39.74,-104.99,'West'),('Boston','MA',42.36,-71.06,'Northeast'),
        ('Nashville','TN',36.16,-86.78,'South'),('Portland','OR',45.52,-122.68,'West'),
        ('Las Vegas','NV',36.17,-115.14,'West'),('Atlanta','GA',33.75,-84.39,'South'),
        ('Miami','FL',25.76,-80.19,'South'),('Minneapolis','MN',44.98,-93.27,'Midwest'),
        ('Tampa','FL',27.95,-82.46,'South'),('Detroit','MI',42.33,-83.05,'Midwest'),
        ('St. Louis','MO',38.63,-90.20,'Midwest'),('Pittsburgh','PA',40.44,-80.00,'Northeast')
),
street_names AS (
    SELECT column1 AS STREET, ROW_NUMBER() OVER (ORDER BY column1) - 1 AS STREET_IDX FROM VALUES
        ('Main St'),('Oak Ave'),('Elm St'),('Maple Dr'),('Pine Rd'),
        ('Cedar Ln'),('Birch Blvd'),('Walnut St'),('Cherry Way'),('Spruce Ct'),
        ('Park Ave'),('Lake Dr'),('River Rd'),('Hill St'),('Valley Blvd'),
        ('Sunset Dr'),('Forest Ave'),('Meadow Ln'),('Spring St'),('Garden Way')
),
nums AS (
    SELECT SEQ4() + 1 AS N FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
SELECT
    n.N AS ADDRESS_KEY,
    ABS(MOD(HASH(n.N), 9900) + 100)::VARCHAR || ' ' || s.STREET AS STREET_LINE1,
    CASE WHEN MOD(n.N, 5) = 0 THEN 'Apt ' || ABS(MOD(HASH(n.N * 7), 500) + 1)::VARCHAR ELSE NULL END AS STREET_LINE2,
    c.CITY,
    c.STATE_PROVINCE,
    LPAD(ABS(MOD(HASH(n.N * 3), 90000) + 10000)::VARCHAR, 5, '0') AS POSTAL_CODE,
    'United States' AS COUNTRY,
    ROUND(c.BASE_LAT + (MOD(HASH(n.N * 11), 100) - 50) / 500.0, 4) AS LATITUDE,
    ROUND(c.BASE_LON + (MOD(HASH(n.N * 13), 100) - 50) / 500.0, 4) AS LONGITUDE,
    CASE MOD(n.N, 3) WHEN 0 THEN 'Residential' WHEN 1 THEN 'Commercial' ELSE 'PO Box' END AS ADDRESS_TYPE,
    c.REGION
FROM nums n
JOIN cities c ON MOD(n.N - 1, 30) = c.CITY_IDX
JOIN street_names s ON MOD(FLOOR((n.N - 1) / 30), 20) = s.STREET_IDX;

-- DIM_CUSTOMER: 5,000 synthetic customers
INSERT INTO PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CUSTOMER
WITH first_names AS (
    SELECT column1 AS FNAME, ROW_NUMBER() OVER (ORDER BY column1) - 1 AS IDX FROM VALUES
        ('James'),('Mary'),('Robert'),('Patricia'),('John'),('Jennifer'),('Michael'),('Linda'),
        ('David'),('Elizabeth'),('William'),('Barbara'),('Richard'),('Susan'),('Joseph'),('Jessica'),
        ('Thomas'),('Sarah'),('Christopher'),('Karen'),('Daniel'),('Lisa'),('Matthew'),('Nancy'),
        ('Anthony'),('Betty'),('Mark'),('Margaret'),('Donald'),('Sandra')
),
last_names AS (
    SELECT column1 AS LNAME, ROW_NUMBER() OVER (ORDER BY column1) - 1 AS IDX FROM VALUES
        ('Smith'),('Johnson'),('Williams'),('Brown'),('Jones'),('Garcia'),('Miller'),('Davis'),
        ('Rodriguez'),('Martinez'),('Hernandez'),('Lopez'),('Gonzalez'),('Wilson'),('Anderson'),
        ('Thomas'),('Taylor'),('Moore'),('Jackson'),('Martin'),('Lee'),('Perez'),('Thompson'),
        ('White'),('Harris'),('Sanchez'),('Clark'),('Ramirez'),('Lewis'),('Robinson')
),
companies AS (
    SELECT column1 AS COMP, ROW_NUMBER() OVER (ORDER BY column1) - 1 AS IDX FROM VALUES
        ('Acme Corp'),('GlobalTech'),('Prime Goods'),('Swift Supply'),('Metro Retail'),
        ('Summit LLC'),('Pinnacle Inc'),('Atlas Trading'),('Nexus Co'),('Apex Solutions')
),
nums AS (
    SELECT SEQ4() + 1 AS N FROM TABLE(GENERATOR(ROWCOUNT => 5000))
)
SELECT
    n.N AS CUSTOMER_KEY,
    'CUST-' || LPAD(n.N::VARCHAR, 6, '0') AS CUSTOMER_ID,
    f.FNAME AS FIRST_NAME,
    l.LNAME AS LAST_NAME,
    CASE WHEN MOD(n.N, 4) = 0 THEN co.COMP ELSE NULL END AS COMPANY_NAME,
    CASE WHEN MOD(n.N, 4) = 0 THEN 'Business' ELSE 'Individual' END AS CUSTOMER_TYPE,
    LOWER(f.FNAME) || '.' || LOWER(l.LNAME) || n.N::VARCHAR || '@email.com' AS EMAIL,
    '555-' || LPAD(ABS(MOD(HASH(n.N * 17), 9000) + 1000)::VARCHAR, 4, '0') || '-' || LPAD(ABS(MOD(HASH(n.N * 19), 9000) + 1000)::VARCHAR, 4, '0') AS PHONE,
    DATEADD(DAY, -ABS(MOD(HASH(n.N * 23), 1095)), '2025-12-31'::DATE) AS REGISTRATION_DATE,
    CASE MOD(n.N, 5) WHEN 0 THEN 'Platinum' WHEN 1 THEN 'Gold' WHEN 2 THEN 'Silver' WHEN 3 THEN 'Bronze' ELSE 'Standard' END AS LOYALTY_TIER
FROM nums n
JOIN first_names f ON MOD(n.N - 1, 30) = f.IDX
JOIN last_names l ON MOD(FLOOR((n.N - 1) / 30), 30) = l.IDX
JOIN companies co ON MOD(n.N - 1, 10) = co.IDX;

-- =============================================================================
-- POPULATE FACT TABLE: 1,000,000 shipments
-- =============================================================================

INSERT INTO PL_AKHRAMKO_DB.DELIVERY_DW.FACT_SHIPMENTS
WITH nums AS (
    SELECT SEQ4() + 1 AS N FROM TABLE(GENERATOR(ROWCOUNT => 1000000))
),
base AS (
    SELECT
        n.N AS SHIPMENT_KEY,
        'TRK' || LPAD(n.N::VARCHAR, 12, '0') AS TRACKING_NUMBER,
        DATEADD(DAY, ABS(MOD(HASH(n.N), 1095)), '2023-01-01'::DATE) AS SHIP_DATE,
        ABS(MOD(HASH(n.N * 3), 10000)) + 1 AS ORIGIN_ADDRESS_KEY,
        ABS(MOD(HASH(n.N * 5), 10000)) + 1 AS DEST_ADDRESS_KEY,
        ABS(MOD(HASH(n.N * 7), 5000)) + 1 AS SENDER_CUSTOMER_KEY,
        ABS(MOD(HASH(n.N * 11), 5000)) + 1 AS RECEIVER_CUSTOMER_KEY,
        ABS(MOD(HASH(n.N * 13), 10)) + 1 AS CARRIER_KEY,
        ABS(MOD(HASH(n.N * 17), 10)) + 1 AS SERVICE_TYPE_KEY,
        ABS(MOD(HASH(n.N * 19), 10)) + 1 AS PACKAGE_TYPE_KEY,
        CASE
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 65 THEN 6   -- Delivered (65%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 70 THEN 4   -- In Transit (5%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 75 THEN 5   -- Out for Delivery (5%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 80 THEN 3   -- At Sorting (5%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 85 THEN 2   -- Picked Up (5%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 88 THEN 7   -- Failed (3%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 91 THEN 8   -- Returned (3%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 93 THEN 1   -- Created (2%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 95 THEN 9   -- Lost (2%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 97 THEN 10  -- Damaged (2%)
            WHEN ABS(MOD(HASH(n.N * 23), 100)) < 99 THEN 11  -- Held (2%)
            ELSE 12                                             -- Redirected (1%)
        END AS STATUS_KEY,
        ROUND(0.1 + ABS(MOD(HASH(n.N * 29), 2000)) / 100.0, 2) AS WEIGHT_KG,
        ROUND(5.0 + ABS(MOD(HASH(n.N * 31), 50000)) / 100.0, 2) AS DECLARED_VALUE_USD,
        ABS(MOD(HASH(n.N * 37), 3000)) + 50 AS DISTANCE_KM_RAW,
        ABS(MOD(HASH(n.N * 41), 3)) + 1 AS DELIVERY_ATTEMPTS
    FROM nums n
)
SELECT
    b.SHIPMENT_KEY,
    b.TRACKING_NUMBER,
    TO_NUMBER(TO_CHAR(b.SHIP_DATE, 'YYYYMMDD')) AS SHIP_DATE_KEY,
    CASE WHEN b.STATUS_KEY IN (6, 7, 8, 10) THEN
        TO_NUMBER(TO_CHAR(DATEADD(DAY,
            CASE
                WHEN b.SERVICE_TYPE_KEY = 4 THEN 0
                WHEN b.SERVICE_TYPE_KEY = 3 THEN 1
                WHEN b.SERVICE_TYPE_KEY IN (2, 6) THEN ABS(MOD(HASH(b.SHIPMENT_KEY * 43), 3)) + 1
                WHEN b.SERVICE_TYPE_KEY IN (1, 5) THEN ABS(MOD(HASH(b.SHIPMENT_KEY * 47), 7)) + 2
                WHEN b.SERVICE_TYPE_KEY = 8 THEN ABS(MOD(HASH(b.SHIPMENT_KEY * 53), 14)) + 5
                ELSE ABS(MOD(HASH(b.SHIPMENT_KEY * 59), 5)) + 1
            END, b.SHIP_DATE), 'YYYYMMDD'))
    ELSE NULL END AS DELIVERY_DATE_KEY,
    b.ORIGIN_ADDRESS_KEY,
    b.DEST_ADDRESS_KEY,
    b.SENDER_CUSTOMER_KEY,
    b.RECEIVER_CUSTOMER_KEY,
    b.CARRIER_KEY,
    b.SERVICE_TYPE_KEY,
    b.PACKAGE_TYPE_KEY,
    b.STATUS_KEY,
    b.WEIGHT_KG,
    b.DECLARED_VALUE_USD,
    ROUND(3.50 + b.WEIGHT_KG * 1.2 + b.DISTANCE_KM_RAW * 0.005
        + CASE b.SERVICE_TYPE_KEY
            WHEN 4 THEN 25.0 WHEN 3 THEN 15.0 WHEN 2 THEN 8.0 WHEN 6 THEN 5.0
            WHEN 7 THEN 50.0 WHEN 8 THEN 12.0 WHEN 10 THEN 3.0 ELSE 0.0
          END, 2) AS SHIPPING_COST_USD,
    CASE WHEN b.SERVICE_TYPE_KEY IN (3, 4, 6) THEN ROUND(b.DECLARED_VALUE_USD * 0.02, 2) ELSE 0.0 END AS INSURANCE_COST_USD,
    b.DISTANCE_KM_RAW AS DISTANCE_KM,
    CASE
        WHEN b.SERVICE_TYPE_KEY = 4 THEN 0
        WHEN b.SERVICE_TYPE_KEY = 3 THEN 1
        WHEN b.SERVICE_TYPE_KEY IN (2, 6) THEN 2
        WHEN b.SERVICE_TYPE_KEY IN (1, 5) THEN 5
        WHEN b.SERVICE_TYPE_KEY = 7 THEN 7
        WHEN b.SERVICE_TYPE_KEY = 8 THEN 14
        ELSE 3
    END AS TRANSIT_DAYS_PLANNED,
    CASE WHEN b.STATUS_KEY IN (6, 7, 8, 10) THEN
        CASE
            WHEN b.SERVICE_TYPE_KEY = 4 THEN 0
            WHEN b.SERVICE_TYPE_KEY = 3 THEN 1
            WHEN b.SERVICE_TYPE_KEY IN (2, 6) THEN ABS(MOD(HASH(b.SHIPMENT_KEY * 43), 3)) + 1
            WHEN b.SERVICE_TYPE_KEY IN (1, 5) THEN ABS(MOD(HASH(b.SHIPMENT_KEY * 47), 7)) + 2
            WHEN b.SERVICE_TYPE_KEY = 8 THEN ABS(MOD(HASH(b.SHIPMENT_KEY * 53), 14)) + 5
            ELSE ABS(MOD(HASH(b.SHIPMENT_KEY * 59), 5)) + 1
        END
    ELSE NULL END AS TRANSIT_DAYS_ACTUAL,
    b.DELIVERY_ATTEMPTS,
    CASE WHEN b.STATUS_KEY = 8 THEN TRUE ELSE FALSE END AS IS_RETURN,
    CASE WHEN b.STATUS_KEY = 10 THEN TRUE ELSE FALSE END AS IS_DAMAGED,
    CASE WHEN b.STATUS_KEY = 9 THEN TRUE ELSE FALSE END AS IS_LOST
FROM base b;

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- Row counts
SELECT 'DIM_DATE' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM PL_AKHRAMKO_DB.DELIVERY_DW.DIM_DATE
UNION ALL SELECT 'DIM_ADDRESS', COUNT(*) FROM PL_AKHRAMKO_DB.DELIVERY_DW.DIM_ADDRESS
UNION ALL SELECT 'DIM_CUSTOMER', COUNT(*) FROM PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CUSTOMER
UNION ALL SELECT 'DIM_CARRIER', COUNT(*) FROM PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CARRIER
UNION ALL SELECT 'DIM_SERVICE_TYPE', COUNT(*) FROM PL_AKHRAMKO_DB.DELIVERY_DW.DIM_SERVICE_TYPE
UNION ALL SELECT 'DIM_PACKAGE_TYPE', COUNT(*) FROM PL_AKHRAMKO_DB.DELIVERY_DW.DIM_PACKAGE_TYPE
UNION ALL SELECT 'DIM_STATUS', COUNT(*) FROM PL_AKHRAMKO_DB.DELIVERY_DW.DIM_STATUS
UNION ALL SELECT 'FACT_SHIPMENTS', COUNT(*) FROM PL_AKHRAMKO_DB.DELIVERY_DW.FACT_SHIPMENTS
ORDER BY 1;

-- FK integrity check (all should be 0)
SELECT
    SUM(CASE WHEN d.DATE_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_SHIP_DATES,
    SUM(CASE WHEN oa.ADDRESS_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_ORIGINS,
    SUM(CASE WHEN da.ADDRESS_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_DESTINATIONS,
    SUM(CASE WHEN sc.CUSTOMER_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_SENDERS,
    SUM(CASE WHEN rc.CUSTOMER_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_RECEIVERS,
    SUM(CASE WHEN cr.CARRIER_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_CARRIERS,
    SUM(CASE WHEN st.SERVICE_TYPE_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_SERVICE_TYPES,
    SUM(CASE WHEN pt.PACKAGE_TYPE_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_PACKAGE_TYPES,
    SUM(CASE WHEN s.STATUS_KEY IS NULL THEN 1 ELSE 0 END) AS ORPHAN_STATUSES
FROM PL_AKHRAMKO_DB.DELIVERY_DW.FACT_SHIPMENTS f
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_DATE d ON f.SHIP_DATE_KEY = d.DATE_KEY
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_ADDRESS oa ON f.ORIGIN_ADDRESS_KEY = oa.ADDRESS_KEY
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_ADDRESS da ON f.DEST_ADDRESS_KEY = da.ADDRESS_KEY
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CUSTOMER sc ON f.SENDER_CUSTOMER_KEY = sc.CUSTOMER_KEY
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CUSTOMER rc ON f.RECEIVER_CUSTOMER_KEY = rc.CUSTOMER_KEY
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_CARRIER cr ON f.CARRIER_KEY = cr.CARRIER_KEY
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_SERVICE_TYPE st ON f.SERVICE_TYPE_KEY = st.SERVICE_TYPE_KEY
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_PACKAGE_TYPE pt ON f.PACKAGE_TYPE_KEY = pt.PACKAGE_TYPE_KEY
LEFT JOIN PL_AKHRAMKO_DB.DELIVERY_DW.DIM_STATUS s ON f.STATUS_KEY = s.STATUS_KEY;
