import pandas as pd
from sqlalchemy import create_engine
import config
from lookup_data import aqi_parameter_breakpoints

def format_results(results):
    """
    results: list of rows
    Pivots them, performs look operation to convert code to text, aggregate and pretty print
    """
    results_formatted = [('No Data' if x[0] == -1 else 'Good' if x[0] == 0 else 'Unhealthy', x[1], x[2]) for x in results]
    df = pd.DataFrame(results_formatted)
    df.columns = ['air_quality','wa_or_not','pct']
    aggregated_df = df.groupby(['air_quality','wa_or_not'])['pct'].agg('sum')
    print(aggregated_df.unstack())

def process_pollutant_aqi_breakpoints():
    """
    Loads the aqi breakpoints data stored as dictionary and loads it into Redshift table
    """
    breakpoints_df = pd.DataFrame()
    air_quality_pollutants = config.air_quality_pollutants

    for pollutant in air_quality_pollutants:
        breakpoints = aqi_parameter_breakpoints[pollutant]["breakpoints"]
        pollutant_name = aqi_parameter_breakpoints[pollutant]["name"]

        for key in breakpoints.keys():
            temp_df = pd.DataFrame.from_dict(breakpoints[key], orient='index', columns=['min', 'max'])
            temp_df['aqi_category'] = temp_df.index
            temp_df['sample_duration'] = key
            temp_df['pollutant_code'] = pollutant
            temp_df['pollutant_name'] = pollutant_name
            breakpoints_df = breakpoints_df.append(temp_df)
        print("Finished processing {} ({})".format(pollutant_name,pollutant))
    redshift_engine = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}" \
                                .format(config.USER,config.PASSWORD,config.HOST,config.PORT,config.DBNAME))
    breakpoints_df.to_sql('pollutant_aqi_breakpoints',con=redshift_engine,if_exists='replace',index=False)