-- Databricks notebook source
CREATE OR REPLACE TABLE `databricks-catalog`.silver.DimProductCategory (
  ProductCategory STRING NOT NULL,
  PRIMARY KEY (ProductCategoryId)
)

-- COMMAND ----------

INSERT OVERWRITE `databricks-catalog`.silver.DimProductCategory
SELECT ProductCategory
FROM `databricks-catalog`.bronze.retailsalesdata
GROUP BY ProductCategory
