# Databricks notebook source
# MAGIC %run "../WeatherAPI/utils/configuration"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../WeatherAPI/utils/functions"

# COMMAND ----------

weather_df = spark.read.format('delta').load("/mnt/openmeteo/processed/weather")


# COMMAND ----------

base_df = weather_df.withColumn("year", F.year("date")).withColumn("month", F.month("date"))


# COMMAND ----------

# Daily weather indicators

from pyspark.sql.window import Window

# Window function for moving avarage
window_spec = Window.partitionBy("latitude", "longitude").orderBy("date").rowsBetween(-3, 3)
                                                                              
weather_daily_kpi_df = base_df \
    .withColumn("temperature_range", F.round(F.col("temperature_max") - F.col("temperature_min"), 1)) \
    .withColumn("is_rainy_day", F.when(F.col("rainfall") > 0.0, 1).otherwise(0)) \
    .withColumn("is_windy_day", F.when(F.col("windspeed_max") > 10, 1).otherwise(0)) \
    .withColumn("avg_temp_7d", F.round(F.avg("temperature_max").over(window_spec), 1))

weather_daily_kpi_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"weather_presentation.daily_kpi")


# COMMAND ----------

# Monthly weather indicators
weather_monthly_summary_df = base_df.groupBy(
    "latitude", "longitude", "timezone", "year", "month"
).agg(
    F.round(F.avg("temperature_max"), 1).alias("avg_temp_max"),
    F.round(F.avg("temperature_min"), 1).alias("avg_temp_min"),
    F.max("temperature_max").alias("max_temp_recorded"),
    F.min("temperature_min").alias("min_temp_recorded"),
    F.round(F.sum("rainfall"), 1).alias("total_rainfall"),
    F.round(F.avg("windspeed_max"), 1).alias("avg_windspeed_max")
)

weather_monthly_summary_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"weather_presentation.monthly_summary")


# COMMAND ----------

# Yearly weather trends

weather_trends_df = weather_daily_kpi_df \
    .groupBy("latitude", "longitude", "timezone", "year") \
    .agg(
        F.round(F.avg("temperature_max"), 1).alias("avg_temp_max"),
        F.round(F.avg("temperature_min"), 1).alias("avg_temp_min"),
        F.round(F.max("temperature_max"), 1).alias("max_temp_recorded"),
        F.round(F.min("temperature_min"), 1).alias("min_temp_recorded"),
        F.round(F.sum("rainfall"), 1).alias("total_rainfall"),
        F.round(F.avg("windspeed_max"), 2).alias("avg_windspeed_max")
    )


weather_trends_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("weather_presentation.yearly_trends")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather_presentation.daily_kpi

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather_presentation.monthly_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather_presentation.yearly_trends