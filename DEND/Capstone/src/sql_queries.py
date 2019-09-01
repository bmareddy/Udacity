import config

# REDSHIFT COPY TEMPLATE
def prepare_copy_statement(table,file,schema="",iam_role="",format="",delimiter="",ignore_headers=1):
    if not schema:
        schema = config.DEFAULT_SCHEMA
    if not iam_role:
        iam_role = config.ARN
    if not format:
        format=config.DEFAULT_OUTPUT_FILE_FORMAT
    if not delimiter:
        delimiter = ','

    sql_stmt = f"""
                COPY {schema}.{table}
                FROM '{file}'
                IAM_ROLE '{iam_role}'
                FORMAT AS {format}
                DELIMITER '{delimiter}'
                IGNOREHEADER {ignore_headers};
                """
    return sql_stmt

# QUERY TO LOAD air_quality FACT TABLE
load_air_quality_query = """
    INSERT INTO public.air_quality (state_code, county_code, date, aqi_category)
    SELECT aqs.state_code, aqs.county_code, aqs.date, MAX(ISNULL(CAST(pab.aqi_category AS SMALLINT),6)) AS aqi_category
    FROM (
        SELECT *, 
        CASE WHEN mean < 0 THEN 0 ELSE mean END as clean_mean,
        CASE WHEN first_max < 0 THEN 0 ELSE first_max END as clean_first_max
        FROM air_quality_staging
        WHERE NOT (
                    (pollutant_code in (42101, 88101) AND sample_duration_std = '1-hour') OR
                    (pollutant_code = 42401 AND sample_duration_std = '24-hour')
                )
        )aqs
    LEFT JOIN pollutant_aqi_breakpoints pab
    ON aqs.pollutant_code = pab.pollutant_code
    AND aqs.sample_duration_std = pab.sample_duration
    AND aqs.clean_mean >= pab.min AND aqs.clean_mean < pab.max
    GROUP BY aqs.state_code, aqs.county_code, aqs.date;
"""

# QUERY THAT ANSWERS THE MAIN QUESTION THAT THE PROJECT IS SET OUT
analysis_query = """
    SELECT aqi_category, white_county_indicator, (CAST(day_count AS FLOAT) * 100) / sum(day_count) OVER (PARTITION BY white_county_indicator) AS pct
    FROM (
        SELECT ISNULL(aq.aqi_category,-1) AS aqi_category, cd.white_county_indicator, COUNT(1) day_count
        FROM (
            SELECT c.state_code, c.state_name, c.county_code, c.county_name, c.white_county_indicator, d.date
            FROM public.census c
            CROSS JOIN (SELECT DISTINCT date FROM public.air_quality) d
        ) cd
        LEFT JOIN public.air_quality aq
            ON cd.state_code = aq.state_code
            AND cd.county_code = aq.county_code
            AND cd.date = aq.date
        GROUP BY ISNULL(aq.aqi_category,-1), cd.white_county_indicator
    ) final;
 """