# Databricks notebook source
dbutils.fs.ls("/Volumes/workspace/default/climate_raw")


# COMMAND ----------

global_temp_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/default/climate_raw/GlobalTemperatures.csv")
)


# COMMAND ----------

global_temp_df.printSchema()
global_temp_df.show(5, truncate=False)


# COMMAND ----------

(
    global_temp_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("bronze_global_temperatures")
)


# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)
spark.sql("SELECT * FROM bronze_global_temperatures LIMIT 5").show(truncate=False)


# COMMAND ----------

country_temp_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/default/climate_raw/GlobalLandTemperaturesByCountry.csv")
)


# COMMAND ----------

country_temp_df.printSchema()
country_temp_df.show(5, truncate=False)



# COMMAND ----------

(
    country_temp_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("bronze_land_temperatures_country")
)


# COMMAND ----------

state_temp_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/default/climate_raw/GlobalLandTemperaturesByState.csv")
)

state_temp_df.printSchema()
state_temp_df.show(5, truncate=False)

(
    state_temp_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("bronze_land_temperatures_state")
)


# COMMAND ----------

city_temp_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("/Volumes/workspace/default/climate_raw/GlobalLandTemperaturesByMajorCity.csv")
)

city_temp_df.printSchema()
city_temp_df.show(5, truncate=False)

(
    city_temp_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("bronze_land_temperatures_city")
)


# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)
