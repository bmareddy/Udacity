## Is Air Quality worse where racial minorities live in the US?

This project attempts to understand and answer if there is a relationship between Air Quality and the population distribution in the United States. This project is a course requirement for the Data Engineering Nanodegree from Udacity. This project covers Data Modeling, Data Wrangling using Apache Spark and analysis using AWS Redshift.

### Data

The data sets used for the project are:
1. Daily Measurements of pollutant gases and particulate matter from 2016-2019. Sourced AQS Datamart by U.S. EPA. Each data set contains 4 files (one per year).
    a. Ozone (row count ~1,246,405 )
    b. Carbon Monoxide (row count ~633,903 )
    c. Sulphur Dioxide (row count ~1,061,586 )
    d. Nitrogen Dioxide (row count ~483,423 )
    e. PM2.5 (row count ~1,482,932 )
    f. PM10 (row count ~537,456 )
2. Population and population estimates by age, race and gender from 2010 through 2018. Sourced from U.S. Census Bureau. This data set contains 1 file with a row count of 656,678

### Scope

The goal of this project is combine the datasets mentioned above and analyze / compare the air quality in counties with non-white majority populations (based on 2018 est.) in the United States. The output of analysis will produce % of days of Good, Moderate and Unhealthy air between counties with white majority and non-white majority populations.

Example Output:
```
-------------------------------------
|           WA      |       Non-WA  |
-------------------------------------
|Good       30%     |       30%     |
|Moderate   30%     |       30%     |
|Unhealthy  30%     |       30%     |
|No Data    10%     |       10%     |
-------------------------------------
```

### Data Model

The data model will contain tables in production as following:

1. census (state_code, state_name, county_code, county_name, year, age_group, total_population, white_male, white_female, white_percent, white_county_indicator)
2. air_quality (state_code, county_code, date, aqi_category)
3. pollutant_aqi_breakpoints (pollutant_code, pollutant_name, sample_duration, aqi_category, min_value, max_value)

### Technology Choices

File ingestion, cleaning and validating is performed using Apache Spark
The data is then copied to Amazon Redshift for analysis

### ETL Approach

The following steps are taken to take the raw data and produce the desired output

#### Census Data
1. Read census file into spark dataframe
2. Filter out to `year == 11` and `agegrp == 0` to get to 2018 total level data
3. Extract the subset of columns `state, stname, county, ctyname, year, agegrp, tot_pop, wa_male + wa_female as tot_wa_pop` and copy to redshift table `census`

#### Air Quality Data
1. For each pollutant, read all four files (2016-2019) of data into a spark dataframe
2. Aggregate the data on `state_code, county_code, date, parameter_code, sample_duration` and get `max` of `mean and first_max` values
3. Apply the following rules based on the pollutant to aggregate to `state_code, county_code, date, parameter_code`.
     a. Areas are generally required to report the AQI based on 8-hour ozone values. However, there are a small number of areas where an AQI based on 1-hour ozone values would be more precautionary. In these cases, in addition to calculating the 8-hour ozone index value, the 1-hour ozone value may be calculated, and the maximum of the two values reported.
     b. 8-hour O3 values do not define higher AQI values (≥ 301). AQI values of 301 or higher are calculated with 1-hour O3 concentrations.
     c. 1-hour SO2 values do not define higher AQI values (≥ 200). AQI values of 200 or greater are calculated with 24-hour SO2 concentrations.
4. Copy the output dataframe to redshift table `air_quality_staging`
5. Copy the `parameter_aqi_breakpoints.csv` file to redshift table `parameter_aqi_breakpoints`
6. Join `air_quality_staging` with `parameter_aqi_breakpoints` on `parameter_code` and `mean between <range_low> and <range_high>` to compute `aqi_category` value for each row in `air_quality_staging`. Insert the output data into `air_quality` table

#### Analysis
Join `air_quality` and `census` tables on `state_code and county_code` and perform aggregations to output the derived result, as specified above

### Additional Considerations

Q: The data was increased by 100x.
A: Currently, the process is designed to read all of the pollution measurement data at the same time. If the data were to increase by 100x, I would change this to read one year at a time (or 1 month or 1 day), so the process can scale well. If speedy processing becomes necessary, I will add additional spark nodes to process all of the data at the same time.

Q: The pipelines would be run on a daily basis by 7 am every day.
A: This will require updating the *Air Quality Data* section of the ETL approach above, to assume a new file (or even a YTD file) for each pollutant is available and process that file every day at 7 am. Other sections will remain as-is. I will write an airflow operator `ingest_pollutant_data` that encapsulates the steps common to all pollutant

Q: The database needed to be accessed by 100+ people.
A: Enable Redshift to auto-scale based on the demand. If the demand continues to increase, replicate the database in multiple regions so the load is distributed.

### Future Improvements to the code
1. Implement logging
2. Implement Data Quality checks in `spark_etl.py`
3. Debug errors with `parquet` file copy to redshift, and remove dependency on pandas.Dataframe.to_csv()
3. Find ways to leverage air quality measurement sites and map them to counties without data

### Run Instructions

1. Execute `create_tables.sql` on the Redshift instance
2. Submit `capstone.py` as spark job

### References
1. https://aqs.epa.gov/aqsweb/documents/data_mart_welcome.html
2. https://aqs.epa.gov/aqsweb/airdata/download_files.html#Daily
3. https://aqs.epa.gov/aqsweb/airdata/FileFormats.html
4. https://www2.census.gov/programs-surveys/popest/datasets/2010-2018/counties/asrh/



