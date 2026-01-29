# Databricks notebook source
gold_country_df = spark.table("gold_climate_country_indicators")


# COMMAND ----------

ml_df = gold_country_df.select(
    "year",
    "avg_yearly_temperature",
    "historical_avg_temperature",
    "temperature_anomaly",
    "high_climate_risk"
)


# COMMAND ----------

ml_df = ml_df.dropna()


# COMMAND ----------

train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=[
        "year",
        "avg_yearly_temperature",
        "historical_avg_temperature",
        "temperature_anomaly"
    ],
    outputCol="features"
)

train_data = assembler.transform(train_df)
test_data = assembler.transform(test_df)


# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="high_climate_risk"
)

lr_model = lr.fit(train_data)


# COMMAND ----------

predictions = lr_model.transform(test_data)


# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(
    labelCol="high_climate_risk",
    metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)
auc


# COMMAND ----------

predictions.groupBy(
    "high_climate_risk", "prediction"
).count().show()


# COMMAND ----------

lr_model.coefficients


# COMMAND ----------

import mlflow
import mlflow.spark

mlflow.set_experiment("/Shared/climate_risk_country_model")


# COMMAND ----------

import os

os.environ["MLFLOW_DFS_TMP"] = "/Volumes/workspace/default/climate_raw/mlflow_tmp"


# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

mlflow.set_experiment("/Shared/climate_risk_country_model")

with mlflow.start_run():

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="high_climate_risk"
    )

    lr_model = lr.fit(train_data)

    predictions = lr_model.transform(test_data)

    evaluator = BinaryClassificationEvaluator(
        labelCol="high_climate_risk",
        metricName="areaUnderROC"
    )

    auc = evaluator.evaluate(predictions)

    mlflow.log_metric("auc", auc)

    mlflow.spark.log_model(
        lr_model,
        artifact_path="logistic_regression_model"
    )


# COMMAND ----------

gold_country_df = spark.table("gold_climate_country_indicators")


# COMMAND ----------

ml_df = gold_country_df.select(
    "year",
    "avg_yearly_temperature",
    "historical_avg_temperature",
    "temperature_anomaly",
    "high_climate_risk"
).dropna()


# COMMAND ----------

train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=[
        "year",
        "avg_yearly_temperature",
        "historical_avg_temperature",
        "temperature_anomaly"
    ],
    outputCol="features"
)

train_data = assembler.transform(train_df)
test_data = assembler.transform(test_df)


# COMMAND ----------

import mlflow.spark

model_uri = "models:/logistic_regression_model/latest"
lr_model = mlflow.spark.load_model(model_uri)


# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="high_climate_risk"
)
lr_model = lr.fit(train_data)


# COMMAND ----------

predictions = lr_model.transform(test_data)


# COMMAND ----------

prediction_df = predictions.select(
    "year",
    "avg_yearly_temperature",
    "historical_avg_temperature",
    "temperature_anomaly",
    "high_climate_risk",
    "prediction",
    "probability"
)


# COMMAND ----------

(
    prediction_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("gold_country_climate_predictions")
)


# COMMAND ----------

spark.sql(
    "SELECT * FROM gold_country_climate_predictions LIMIT 5"
).show(truncate=False)


# COMMAND ----------

