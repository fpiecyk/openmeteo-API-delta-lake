# Databricks notebook source
# MAGIC %run "../WeatherAPI/utils/configuration"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../WeatherAPI/utils/functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, ArrayType
weather_schema = StructType([

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("generationtime_ms", DoubleType(), True),
    StructField("utc_offset_seconds", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_abbreviation", StringType(), True),
    StructField("elevation", DoubleType(), True),
    
    StructField("daily_units", StructType([
        StructField("time", StringType(), True),
        StructField("temperature_2m_max", StringType(), True),
        StructField("temperature_2m_min", StringType(), True),
        StructField("precipitation_sum", StringType(), True),
        StructField("windspeed_10m_max", StringType(), True)
    ]), True),

    StructField("daily", StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("temperature_2m_max", ArrayType(DoubleType()), True),
        StructField("temperature_2m_min", ArrayType(DoubleType()), True),
        StructField("precipitation_sum", ArrayType(DoubleType()), True),
        StructField("windspeed_10m_max", ArrayType(DoubleType()), True)
    ]), True)
])

# COMMAND ----------

historical_years = [2019, 2020, 2021, 2022, 2023, 2024]
weather_df = read_historical_weather(raw_folder_path, historical_years)

# COMMAND ----------

from pyspark.sql.functions import arrays_zip, explode, col

# Join all columns in one structure
weather_df_zipped = weather_df.withColumn(
    "daily_combined",
    arrays_zip(
        col("daily.time"),
        col("daily.temperature_2m_max"),
        col("daily.temperature_2m_min"),
        col("daily.precipitation_sum"),
        col("daily.windspeed_10m_max")
    )
)

# COMMAND ----------

weather_final_df = weather_df_zipped.select(
    col("latitude"),
    col("longitude"),
    col("timezone"),
    explode(col("daily_combined")).alias("daily_data")
).select(
    col("latitude"),
    col("longitude"),
    col("timezone"),
    col("daily_data.time").alias("date"),
    col("daily_data.temperature_2m_max").alias("temperature_max"),
    col("daily_data.temperature_2m_min").alias("temperature_min"),
    col("daily_data.precipitation_sum").alias("rainfall"),
    col("daily_data.windspeed_10m_max").alias("windspeed_max")
)

# COMMAND ----------

#For historic save
weather_final_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("weather_processed.weather")

# COMMAND ----------

df = spark.read.format('delta').load("/mnt/openmeteo/processed/weather")
