-- Databricks notebook source
CREATE TABLE IF NOT EXISTS `databricks-catalog`.silver.DimProductCategory (
  ProductCategoryId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  ProductCategory STRING NOT NULL,
  PRIMARY KEY (ProductCategoryId)
)

-- COMMAND ----------

INSERT OVERWRITE `databricks-catalog`.silver.DimProductCategory (ProductCategory)
SELECT
    ProductCategory
FROM `databricks-catalog`.bronze.retailsalesdata
GROUP BY ProductCategory
