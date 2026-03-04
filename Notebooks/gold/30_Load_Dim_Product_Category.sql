-- Databricks notebook source
CREATE TABLE IF NOT EXISTS `databricks-catalog`.gold.DimProductCategory (
  ProductCategoryId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  ProductCategory STRING NOT NULL,
  PRIMARY KEY (ProductCategoryId)
)

-- COMMAND ----------

MERGE INTO `databricks-catalog`.gold.DimProductCategory AS target
USING `databricks-catalog`.silver.DimProductCategory AS source
    ON target.ProductCategory = source.ProductCategory
WHEN NOT MATCHED 
    THEN INSERT (ProductCategory)
    VALUES (source.ProductCategory);
