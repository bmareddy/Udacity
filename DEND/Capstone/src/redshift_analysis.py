from common import execute_redshift_load_query, execute_redshift_select_query
from sql_queries import load_air_quality_query, analysis_query

def load_air_quality():
    execute_redshift_load_query(load_air_quality_query)

def run_analysis():
    data = execute_redshift_select_query(analysis_query)
    return data