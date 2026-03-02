CREATE SCHEMA covid_reporting
GO
CREATE TABLE hospital_admissions (
    country         VARCHAR(100),
    indicator       VARCHAR(100),
    date            DATE,
    year_week       VARCHAR(10),
    value           FLOAT,
    source          VARCHAR(200)
)
GO

CREATE TABLE cases_deaths (
    Reporting_date          DATE,
    day                     INT,
    month                   INT,
    year                    INT,
    Confirmed_cases         INT,
    deaths                  INT,
    Country                 VARCHAR(100),
    country_code_2_digit    VARCHAR(10),
    country_code_3_digit    VARCHAR(10),
    population              INT,
    continent               VARCHAR(50)
)
GO

CREATE TABLE testing (
    country                 VARCHAR(100),
    country_code            VARCHAR(10),
    year_week               VARCHAR(10),
    level                   VARCHAR(50),
    region                  VARCHAR(100),
    region_name             VARCHAR(100),
    new_cases               FLOAT,
    tests_done              FLOAT,
    population              INT,
    testing_rate            FLOAT,
    positivity_rate         FLOAT,
    testing_data_source     VARCHAR(200)
)
GO

CREATE TABLE population (
    country_code            VARCHAR(10),
    age_group               VARCHAR(50),
    population_2019         BIGINT
)
GO

CREATE TABLE cases_by_age (
    country                 VARCHAR(100),
    country_code            VARCHAR(10),
    year_week               VARCHAR(10),
    age_group               VARCHAR(50),
    new_cases               FLOAT,
    population              INT,
    rate_14_day_per_100k    FLOAT,
    source                  VARCHAR(200)
);
