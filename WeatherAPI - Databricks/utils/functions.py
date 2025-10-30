# Databricks notebook source
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F

def save_weather_data(weather_df, v_file_year: int):
    """
    Saves weather data to a managed Delta table.
    If the table does not exist, it will be created.
    If the table exists, new records will be appended instead of overwritten.
    """

    target_table = "weather_processed.weather"

    # Add a year column for traceability or partitioning
    weather_df = weather_df.withColumn("year", F.lit(v_file_year))

    try:
        # Check if the target table already exists
        spark.table(target_table)
        table_exists = True
    except AnalysisException:
        table_exists = False

    if not table_exists:
        # Create the table if it doesn't exist
        weather_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(target_table)
    else:
        # Append new data if the table already exists
        weather_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)


# COMMAND ----------

def read_historical_weather(raw_folder_path, historical_years):
    """
    Read all historical weather JSON files once and return a DataFrame.
    """
    dfs = []

    for year in historical_years:
        path_pattern = f"{raw_folder_path}/{year}/weather_*.json"
        df_year = spark.read \
            .option("header", "true") \
            .schema(weather_schema) \
            .json(path_pattern) \
            .withColumn("year", F.lit(year))
        dfs.append(df_year)

    weather_df = dfs[0]
    for df in dfs[1:]:
        weather_df = weather_df.unionByName(df)

    return weather_df


# COMMAND ----------

from pyspark.sql import functions as F

def read_daily_weather(raw_folder_path, current_year, start_date_str):
    """
    Read only the weather JSON file for the date passed from the pipeline trigger.
    
    Parameters:
    - raw_folder_path: base path to raw data
    - current_year: year of the data
    - start_date_str: date string in format 'yyyy-MM-dd' from pipeline parameter
    """
    path_to_file = f"{raw_folder_path}/{current_year}/*/weather_{start_date_str}.json"

    weather_df = spark.read \
        .option("header", "true") \
        .schema(weather_schema) \
        .json(path_to_file) \
        .withColumn("year", F.lit(current_year))

    return weather_df
