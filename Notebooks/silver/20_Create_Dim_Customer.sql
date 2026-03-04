-- Databricks notebook source
CREATE TABLE `databricks-catalog`.silver.DimCustomer (
  CustomerId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  CustomerSourceId STRING NOT NULL, 
  Gender STRING NOT NULL, 
  Age INT NOT NULL, 
  PRIMARY KEY (CustomerId)
)

-- COMMAND ----------

INSERT OVERWRITE `databricks-catalog`.silver.DimCustomer (CustomerSourceId, Gender, Age)
SELECT
  CustomerId AS CustomerSourceId,
  Gender,
  Age
FROM
  `databricks-catalog`.bronze.RetailSalesData
