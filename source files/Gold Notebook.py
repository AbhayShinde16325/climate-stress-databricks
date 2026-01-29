# Databricks notebook source
silver_country_df = spark.table("silver_land_temperatures_country")


# COMMAND ----------

from pyspark.sql.functions import year, col

country_year_df = silver_country_df.withColumn(
    "year", year(col("date"))
)


# COMMAND ----------

country_year_avg_df = (
    country_year_df
        .groupBy("country", "year")
        .agg(
            {"AverageTemperature": "avg"}
        )
        .withColumnRenamed(
            "avg(AverageTemperature)",
            "avg_yearly_temperature"
        )
)


# COMMAND ----------

from pyspark.sql.functions import avg

country_baseline_df = (
    country_year_avg_df
        .groupBy("country")
        .agg(
            avg("avg_yearly_temperature")
            .alias("historical_avg_temperature")
        )
)


# COMMAND ----------

gold_country_df = (
    country_year_avg_df
        .join(
            country_baseline_df,
            on="country",
            how="inner"
        )
)


# COMMAND ----------

gold_country_df = gold_country_df.withColumn(
    "temperature_anomaly",
    col("avg_yearly_temperature") - col("historical_avg_temperature")
)


# COMMAND ----------

gold_country_df.printSchema()
gold_country_df.show(10, truncate=False)


# COMMAND ----------

(
    gold_country_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("gold_climate_country_indicators")
)


# COMMAND ----------

spark.sql(
    """
    SELECT country, year, temperature_anomaly
    FROM gold_climate_country_indicators
    ORDER BY year DESC
    LIMIT 10
    """
).show(truncate=False)


# COMMAND ----------

gold_country_df = spark.table("gold_climate_country_indicators")


# COMMAND ----------

from pyspark.sql.functions import stddev

country_anomaly_stats_df = (
    gold_country_df
        .groupBy("country")
        .agg(
            stddev("temperature_anomaly")
            .alias("anomaly_stddev")
        )
)



# COMMAND ----------

gold_country_with_stats_df = (
    gold_country_df
        .join(
            country_anomaly_stats_df,
            on="country",
            how="inner"
        )
)


# COMMAND ----------

from pyspark.sql.functions import when,col

gold_country_labeled_df = (
    gold_country_with_stats_df
        .withColumn(
            "high_climate_risk",
            when(
                col("temperature_anomaly") > col("anomaly_stddev"),
                1
            ).otherwise(0)
        )
)



# COMMAND ----------

gold_country_labeled_df.select(
    "country",
    "year",
    "temperature_anomaly",
    "anomaly_stddev",
    "high_climate_risk"
).orderBy("year", ascending=False).show(15, truncate=False)


# COMMAND ----------

(
    gold_country_labeled_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("gold_climate_country_indicators")
)



# COMMAND ----------

spark.sql(
    """
    SELECT high_climate_risk, COUNT(*) AS count
    FROM gold_climate_country_indicators
    GROUP BY high_climate_risk
    """
).show()


# COMMAND ----------

silver_state_df = spark.table("silver_land_temperatures_state")


# COMMAND ----------

from pyspark.sql.functions import year, col

state_year_df = silver_state_df.withColumn(
    "year", year(col("date"))
)


# COMMAND ----------

state_year_avg_df = (
    state_year_df
        .groupBy("country", "state", "year")
        .agg(
            {"AverageTemperature": "avg"}
        )
        .withColumnRenamed(
            "avg(AverageTemperature)",
            "avg_yearly_temperature"
        )
)


# COMMAND ----------

from pyspark.sql.functions import avg

state_baseline_df = (
    state_year_avg_df
        .groupBy("country", "state")
        .agg(
            avg("avg_yearly_temperature")
            .alias("historical_avg_temperature")
        )
)


# COMMAND ----------

gold_state_df = (
    state_year_avg_df
        .join(
            state_baseline_df,
            on=["country", "state"],
            how="inner"
        )
)


# COMMAND ----------

gold_state_df = gold_state_df.withColumn(
    "temperature_anomaly",
    col("avg_yearly_temperature") - col("historical_avg_temperature")
)


# COMMAND ----------

from pyspark.sql.functions import stddev

state_anomaly_stats_df = (
    gold_state_df
        .groupBy("country", "state")
        .agg(
            stddev("temperature_anomaly")
            .alias("anomaly_stddev")
        )
)


# COMMAND ----------

from pyspark.sql.functions import when

gold_state_labeled_df = (
    gold_state_df
        .join(
            state_anomaly_stats_df,
            on=["country", "state"],
            how="inner"
        )
        .withColumn(
            "high_climate_risk",
            when(
                col("temperature_anomaly") > col("anomaly_stddev"),
                1
            ).otherwise(0)
        )
)


# COMMAND ----------

gold_state_labeled_df.select(
    "country",
    "state",
    "year",
    "temperature_anomaly",
    "high_climate_risk"
).orderBy("year", ascending=False).show(15, truncate=False)


# COMMAND ----------

(
    gold_state_labeled_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("gold_climate_state_indicators")
)


# COMMAND ----------

spark.sql(
    """
    SELECT high_climate_risk, COUNT(*) AS count
    FROM gold_climate_state_indicators
    GROUP BY high_climate_risk
    """
).show()


# COMMAND ----------

silver_city_df = spark.table("silver_land_temperatures_city")



# COMMAND ----------

from pyspark.sql.functions import year, month, col

city_time_df = (
    silver_city_df
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
)


# COMMAND ----------

from pyspark.sql.window import Window

city_window = (
    Window
        .partitionBy("country", "city")
        .orderBy("date")
        .rowsBetween(-11, 0)
)


# COMMAND ----------

from pyspark.sql.functions import avg

city_rolling_df = city_time_df.withColumn(
    "rolling_12_month_avg_temp",
    avg(col("AverageTemperature")).over(city_window)
)


# COMMAND ----------

city_baseline_df = (
    city_rolling_df
        .groupBy("country", "city")
        .agg(
            avg("rolling_12_month_avg_temp")
            .alias("historical_avg_temperature")
        )
)


# COMMAND ----------

gold_city_df = (
    city_rolling_df
        .join(
            city_baseline_df,
            on=["country", "city"],
            how="inner"
        )
        .withColumn(
            "temperature_anomaly",
            col("rolling_12_month_avg_temp") - col("historical_avg_temperature")
        )
)


# COMMAND ----------

from pyspark.sql.functions import stddev

city_anomaly_stats_df = (
    gold_city_df
        .groupBy("country", "city")
        .agg(
            stddev("temperature_anomaly")
            .alias("anomaly_stddev")
        )
)


# COMMAND ----------

from pyspark.sql.functions import when

gold_city_labeled_df = (
    gold_city_df
        .join(
            city_anomaly_stats_df,
            on=["country", "city"],
            how="inner"
        )
        .withColumn(
            "high_climate_risk",
            when(
                col("temperature_anomaly") > col("anomaly_stddev"),
                1
            ).otherwise(0)
        )
)


# COMMAND ----------

gold_city_labeled_df.select(
    "country",
    "city",
    "date",
    "rolling_12_month_avg_temp",
    "temperature_anomaly",
    "high_climate_risk"
).orderBy("date", ascending=False).show(15, truncate=False)


# COMMAND ----------

(
    gold_city_labeled_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("gold_climate_city_indicators")
)


# COMMAND ----------

spark.sql(
    """
    SELECT high_climate_risk, COUNT(*) AS count
    FROM gold_climate_city_indicators
    GROUP BY high_climate_risk
    """
).show()


# COMMAND ----------

spark.sql(
    """
    SELECT high_climate_risk, COUNT(*) AS count
    FROM gold_climate_city_indicators
    GROUP BY high_climate_risk
    """
).show()


# COMMAND ----------

