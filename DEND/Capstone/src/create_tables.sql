CREATE TABLE IF NOT EXISTS public.census (
	state_code VARCHAR(2) NOT NULL,
	state_name VARCHAR(256),
	county_code VARCHAR(3) NOT NULL,
	county_name VARCHAR(256),
	"year" SMALLINT,
    age_group INTEGER,
    total_population BIGINT,
    white_male BIGINT,
    white_female BIGINT,
    white_percent FLOAT,
    white_county_indicator BOOLEAN,
    CONSTRAINT pk_census PRIMARY KEY (county_code, state_code, year)
);

CREATE TABLE IF NOT EXISTS public.air_quality_staging (
    state_code VARCHAR(2) NOT NULL,
    county_code VARCHAR(3) NOT NULL,
    "date" DATE NOT NULL,
    sample_duration VARCHAR(255),
    pollutant_code INTEGER NOT NULL,
    mean FLOAT,
    first_max FLOAT,
    sample_duration_std VARCHAR(7) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.air_quality (
    state_code VARCHAR(2) NOT NULL,
    county_code VARCHAR(3) NOT NULL,
    "date" DATE NOT NULL,
    aqi_category INTEGER NOT NULL,
    CONSTRAINT pk_air_quality PRIMARY KEY (county_code, state_code, date)
);

CREATE TABLE IF NOT EXISTS public.pollutant_aqi_breakpoints (
    pollutant_code INTEGER NOT NULL,
    pollutant_name VARCHAR(255),
    sample_duration VARCHAR(7) NOT NULL,
    aqi_category INTEGER NOT NULL,
    min_value FLOAT,
    max_value FLOAT,
    CONSTRAINT pk_pollutant_aqi_breakpoints PRIMARY KEY (pollutant_code, sample_duration)
);