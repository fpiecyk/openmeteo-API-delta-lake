# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists weather_raw cascade;
# MAGIC
# MAGIC create database if not exists weather_raw
# MAGIC location "dbfs:/mnt/openmeteo/raw"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists weather_processed cascade;
# MAGIC
# MAGIC create database if not exists weather_processed
# MAGIC location "/mnt/openmeteo/processed"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists weather_presentation cascade;
# MAGIC
# MAGIC create database if not exists weather_presentation
# MAGIC location "/mnt/openmeteo/presentation"

# COMMAND ----------

# MAGIC %sql
# MAGIC desc database extended weather_processed;

# COMMAND ----------

# MAGIC %md
# MAGIC