from spark_etl import create_spark_session, process_census_data, process_air_quality_data
from pandas_etl import process_pollutant_aqi_breakpoints, format_results
from redshift_analysis import load_air_quality, run_analysis
import config

def main():
    spark = create_spark_session()
    
    census_input_data = "{}/{}".format(config.INPUT_PATH, "cc-est2018-alldata.csv")
    census_output_data = "{}/{}.{}".format(config.OUTPUT_PATH, "census", config.DEFAULT_OUTPUT_FILE_FORMAT)
    process_census_data(spark, census_input_data, census_output_data)

    air_quality_pollutants = config.air_quality_pollutants

    for pollutant in air_quality_pollutants:
        input_data = "{}/{}/{}".format(config.INPUT_PATH, "aqs",str(pollutant))
        output_data = "{}/{}.{}".format(config.OUTPUT_PATH, str(pollutant),config.DEFAULT_OUTPUT_FILE_FORMAT)
        print(f"Input: {input_data}")
        print(f"Output: {output_data}")

        process_air_quality_data(spark, input_data, output_data)

    process_pollutant_aqi_breakpoints()
    load_air_quality()
    results = run_analysis()
    format_results(results)

if __name__ == "__main__":
    main()