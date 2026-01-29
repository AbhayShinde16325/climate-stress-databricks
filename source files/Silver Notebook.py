# Databricks notebook source
bronze_global_df = spark.table("bronze_global_temperatures")


# COMMAND ----------

bronze_global_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import to_date, col

silver_global_df = (
    bronze_global_df
        .withColumn("date", to_date(col("dt")))
        .drop("dt")
)


# COMMAND ----------

silver_global_df = silver_global_df.filter(col("date").isNotNull())


# COMMAND ----------

silver_global_df.printSchema()
silver_global_df.show(5, truncate=False)


# COMMAND ----------

(
    silver_global_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("silver_global_temperatures")
)


# COMMAND ----------

spark.sql("SELECT * FROM silver_global_temperatures LIMIT 5").show(truncate=False)


# COMMAND ----------

bronze_country_df = spark.table("bronze_land_temperatures_country")


# COMMAND ----------

bronze_country_df.printSchema()
bronze_country_df.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import to_date, col

silver_country_df = (
    bronze_country_df
        .withColumn("date", to_date(col("dt")))
        .drop("dt")
)


# COMMAND ----------

silver_country_df = (
    silver_country_df
        .filter(col("date").isNotNull())
        .filter(col("Country").isNotNull())
)


# COMMAND ----------

silver_country_df = silver_country_df.withColumnRenamed(
    "Country", "country"
)


# COMMAND ----------

silver_country_df.printSchema()
silver_country_df.show(5, truncate=False)


# COMMAND ----------

(
    silver_country_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("silver_land_temperatures_country")
)


# COMMAND ----------

spark.sql(
    "SELECT country, date, AverageTemperature FROM silver_land_temperatures_country LIMIT 5"
).show(truncate=False)


# COMMAND ----------

bronze_state_df = spark.table("bronze_land_temperatures_state")


# COMMAND ----------

bronze_state_df.printSchema()
bronze_state_df.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import to_date, col

silver_state_df = (
    bronze_state_df
        .withColumn("date", to_date(col("dt")))
        .drop("dt")
)


# COMMAND ----------

silver_state_df = (
    silver_state_df
        .filter(col("date").isNotNull())
        .filter(col("State").isNotNull())
        .filter(col("Country").isNotNull())
)


# COMMAND ----------

silver_state_df = (
    silver_state_df
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Country", "country")
)


# COMMAND ----------

silver_state_df.printSchema()
silver_state_df.show(5, truncate=False)


# COMMAND ----------

(
    silver_state_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("silver_land_temperatures_state")
)


# COMMAND ----------

spark.sql(
    "SELECT country, state, date, AverageTemperature FROM silver_land_temperatures_state LIMIT 5"
).show(truncate=False)


# COMMAND ----------

bronze_city_df = spark.table("bronze_land_temperatures_city")


# COMMAND ----------

bronze_city_df.printSchema()
bronze_city_df.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import to_date, col

silver_city_df = (
    bronze_city_df
        .withColumn("date", to_date(col("dt")))
        .drop("dt")
)


# COMMAND ----------

silver_city_df = (
    silver_city_df
        .filter(col("date").isNotNull())
        .filter(col("City").isNotNull())
        .filter(col("Country").isNotNull())
)


# COMMAND ----------

silver_city_df = (
    silver_city_df
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("Latitude", "latitude")
        .withColumnRenamed("Longitude", "longitude")
)


# COMMAND ----------

silver_city_df.printSchema()
silver_city_df.show(5, truncate=False)


# COMMAND ----------

(
    silver_city_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("silver_land_temperatures_city")
)


# COMMAND ----------

spark.sql(
    "SELECT city, country, date, AverageTemperature FROM silver_land_temperatures_city LIMIT 5"
).show(truncate=False)


# COMMAND ----------

