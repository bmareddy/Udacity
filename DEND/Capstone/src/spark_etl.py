import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, max, when
from pyspark.sql.types import StringType

import config
from common import execute_redshift_load_query
from sql_queries import prepare_copy_statement
from lookup_data import aqi_parameter_breakpoints

os.environ['AWS_ACCESS_KEY_ID']=config.AWS_ACCESS_KEY_ID
os.environ['AWS_SECRET_ACCESS_KEY']=config.AWS_SECRET_ACCESS_KEY

def standardize_sample_duration(sample_duration):
    """
    sample_duration: a string description of duration at which devices sample and measure pollutants
    returns a standardized string - '24-hour' or '8-hour' or '1-hour'
    """
    durations = ['24','8','1']
    for duration in durations:
        if duration in sample_duration:
            return duration+'-hour'
    return 'unknown'

def create_spark_session():
    spark = SparkSession \
            .builder \
            .appName("AQIandCensus") \
            .getOrCreate()
    return spark

def process_census_data(spark, input_data, output_data):
    """
    spark: spark session object
    input_data: fully qualified path to the census csv file
    ingests, filters, adds a derived column to compute total white and total non-white population by county
    and writes the data to redshift
    """
    census_df = spark.read.csv(input_data, header='true', inferSchema='true')

    #filter data to 2018 census only and for total age group and rename columns
    census_df_filtered = census_df.where("YEAR == 11 and AGEGRP == 0") \
                                .selectExpr("STATE as state_code" \
                                            , "STNAME as state_name" \
                                            , "COUNTY as county_code" \
                                            , "CTYNAME as county_name" \
                                            , "YEAR as year", "AGEGRP as age_group" \
                                            , "TOT_POP as total_population" \
                                            , "WA_MALE as white_male" \
                                            , "WA_FEMALE as white_female")
    
    #add derived column: white majority indicator by county
    census_df_filtered = census_df_filtered \
                        .withColumn("white_percent" \
                            , (census_df_filtered.white_male \
                                + census_df_filtered.white_female) \
                                    / census_df_filtered.total_population)

    census_df_filtered = census_df_filtered \
                        .withColumn("white_county_indicator", \
                            when(col("white_percent") >= 0.5, 1) \
                            .otherwise(0))
    
    # census_df_filtered.write \
    #                 .format('parquet') \
    #                 .mode('overwrite') \
    #                 .save(output_data)

    census_df_filtered.toPandas().to_csv(output_data, encoding='utf-8', index=False)
    copy_to_redshift_query = prepare_copy_statement('census',output_data)
    print(f"Query: {copy_to_redshift_query}")
    execute_redshift_load_query(copy_to_redshift_query)

def process_air_quality_data(spark, input_data, output_data):
    """
    spark: spark session object
    input_data: fully qualified path to directory containing the csv files for a given pollutant
    ingests, aggregates, and incorporates logic to derive accurate measurement value 
    by county an date for each pollutant. Writes output data to redshift
    """
    raw_df = spark.read.csv(f"{input_data}/*.csv", header='true', inferSchema='true')
    raw_df.groupBy("`Parameter Name`").count().show()

    # select only relevant columns and rename
    data_df = raw_df.selectExpr("`State Code` as state_code", \
                            "`County Code` as county_code", \
                            "`Date Local` as date", \
                            "`Sample Duration` as sample_duration", \
                            "`Parameter Code` as pollutant_code", \
                            "`Arithmetic Mean` as mean", \
                            "`1st Max Value` as first_max")
    
    # aggregate to consider only max values of mean and first_max
    data_df = data_df.groupBy("state_code", "county_code", "date", "sample_duration", "pollutant_code") \
                    .agg({"mean":"max", "first_max":"max"}) \
                    .withColumnRenamed("max(mean)","mean") \
                    .withColumnRenamed("max(first_max)","first_max")

    # standardize sample duration. couldn't get this working through udf() so used when()
    data_df = data_df.withColumn("sample_duration_std" \
                        , when(col("sample_duration").like("%24%") | \
                                    (col("sample_duration").like("%3%") & (data_df.pollutant_code == 42401)), "24-hour") \
                            .otherwise(when(col("sample_duration").like("%8%"), "8-hour") \
                                .otherwise(when(col("sample_duration").like("%1%"), "1-hour") \
                                    .otherwise("unknown"))))
    
    # data_df.write \
    #     .format('parquet') \
    #     .mode('overwrite') \
    #     .save(output_data)
    data_df.toPandas().to_csv(output_data, encoding='utf-8', index=False)
    copy_to_redshift_query = prepare_copy_statement('air_quality_staging',output_data)
    print(f"Query: {copy_to_redshift_query}")
    execute_redshift_load_query(copy_to_redshift_query)