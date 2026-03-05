# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC > Create widgets to pass file path

# COMMAND ----------

dbutils.widgets.text("file_name", "")
dbutils.widgets.text("extension", "")
dbutils.widgets.text("source_table", "")

# COMMAND ----------

file_name = dbutils.widgets.get("file_name")
extension = dbutils.widgets.get("extension")
source_table = dbutils.widgets.get("extension")

# COMMAND ----------

# MAGIC %md
# MAGIC > Create 
# MAGIC   - Catalog
# MAGIC   - Schema

# COMMAND ----------

CREATE CATALOG IF NOT EXISTS `databricks-catalog`;
CREATE SCHEMA IF NOT EXISTS `databricks-catalog`.`bronze`;
CREATE SCHEMA IF NOT EXISTS `databricks-catalog`.`silver`;
CREATE SCHEMA IF NOT EXISTS `databricks-catalog`.`gold`;


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Extract [Retail Sales Dataset](https://www.kaggle.com/datasets/mohammadtalib786/retail-sales-dataset) from Kaggle
# MAGIC
# MAGIC - Install required library
# MAGIC - Create python functions to 
# MAGIC   - Prepare schema
# MAGIC   - Extract data from files and store as raw table
# MAGIC

# COMMAND ----------

# MAGIC %pip install kagglehub[pandas-datasets]

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from kagglehub import KaggleDatasetAdapter
import kagglehub

def create_schema_for_raw_data():
    return StructType([
        StructField("TransactionId", IntegerType()),
        StructField("Date", StringType()),
        StructField("CustomerId", StringType()),
        StructField("Gender", StringType()),
        StructField("Age", IntegerType()),
        StructField("ProductCategory", StringType()),
        StructField("Quantity", IntegerType()),
        StructField("UnitPrice", IntegerType()),
        StructField("TotalAmount", IntegerType())
    ])

def get_files(file_name, extension):

    file_path = f"{file_name}.{extension}"

    df = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "mohammadtalib786/retail-sales-dataset",
    file_path,
    )

    (
        spark.createDataFrame(df, schema=create_schema_for_raw_data())
        .write
        .mode("overwrite")
        .saveAsTable(f"`databricks-catalog`.bronze.{source_table}")
    )

# COMMAND ----------

get_files(file_name, extension)
