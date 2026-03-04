-- Databricks notebook source
CREATE OR REPLACE TABLE `databricks-catalog`.silver.DimCustomer (
  CustomerSourceId STRING NOT NULL, 
  Gender STRING NOT NULL, 
  Age INT NOT NULL, 
  PRIMARY KEY (CustomerSourceId)
)

-- COMMAND ----------

INSERT OVERWRITE `databricks-catalog`.silver.DimCustomer
SELECT
  CustomerId AS CustomerSourceId,
  Gender,
  Age
FROM
  `databricks-catalog`.bronze.RetailSalesData
