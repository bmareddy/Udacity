## Is Air Quality worse where racial minorities live in the US?

This project attempts to understand and answer if there is a relationship between Air Quality and the population distribution in the United States. This project is a course requirement for the Data Engineering Nanodegree from Udacity. This project covers Data Modeling, Data Wrangling using Apache Spark and analysis using AWS Redshift.

### Data

The data sets used for the project are:
1. Daily Measurements of pollutant gases and particulate matter from 2016-2019. Sourced AQS Datamart by U.S. EPA. Each data set contains 4 files (one per year).
    a. Ozone (row count ~ )
    b. Carbon Monoxide (row count ~ )
    c. Sulphur Dioxide (row count ~ )
    d. Nitrogen Dioxide (row count ~ )
    e. PM2.5 (row count ~ )
    f. PM10 (roow count ~ )
2. Population and population estimates by age, race and gender from 2010 through 2018. Sourced from U.S. Census Bureau. This data set contains 1 file with a row count of 656,678

### Scope

The goal of this project is combine the datasets mentioned above and analyze / compare the air quality in counties with non-white majority populations (based on 2018 est.) in the United States. The output of analysis will produce % of days of Good, Moderate and Unhealthy air between counties with white majority and non-white majority populations.

Example Output:
-------------------------------------
|           WA      |       Non-WA  |
-------------------------------------
|Good       40%     |       40%     |
|Moderate   30%     |       30%     |
|Unhealthy  30%     |       30%     |
-------------------------------------

### Data Model

The data model will contain tables as following:

1. census (state_code, state_name, county_code, county_name, year, agegrp, tot_pop, tot_wa_pop)
2. air_quality (state_code, county_code, date, parameter_code, parameter_name, mean, first_max, aqi_original, aqi_category)
3. parameter_aqi_breakpoints (parameter_code, min_value, max_value, aqi_category)

### Technology Choices

File ingestion, cleaning and validating is performed using Apache Spark
The data is then copied to Amazon Redshift for analysis

### ETL Approach

### Notes

### References
1. https://aqs.epa.gov/aqsweb/documents/data_mart_welcome.html
2. https://aqs.epa.gov/aqsweb/airdata/download_files.html#Daily
3. https://aqs.epa.gov/aqsweb/airdata/FileFormats.html
4. https://www2.census.gov/programs-surveys/popest/datasets/2010-2018/counties/asrh/



