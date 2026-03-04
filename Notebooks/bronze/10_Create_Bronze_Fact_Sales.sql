-- Databricks notebook source
CREATE TABLE IF NOT EXISTS `databricks-catalog`.bronze.RetailSalesData (
  TransactionId INT,
  Date DATE,
  CustomerId STRING,
  Gender STRING,
  Age INT,
  ProductCategory STRING,
  Quantity INT,
  UnitPrice INT,
  TotalAmount INT
)

-- COMMAND ----------

INSERT OVERWRITE `databricks-catalog`.bronze.RetailSalesData
SELECT 
  TransactionId,
  Date,
  CustomerId,
  Gender,
  Age,
  ProductCategory,
  Quantity,
  UnitPrice,
  TotalAmount
FROM 
  `databricks-catalog`.raw.retail_sales
