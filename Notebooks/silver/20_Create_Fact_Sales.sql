-- Databricks notebook source
CREATE OR REPLACE TABLE `databricks-catalog`.silver.FactSales (
  TransactionId INT NOT NULL,
  Date DATE NOT NULL,
  CustomerId STRING NOT NULL,
  Gender STRING NOT NULL,
  Age INT NOT NULL,
  ProductCategory STRING NOT NULL,
  Quantity INT NOT NULL,
  UnitPrice INT NOT NULL,
  TotalAmount INT NOT NULL,
  PRIMARY KEY (TransactionId)
)

-- COMMAND ----------

INSERT OVERWRITE `databricks-catalog`.silver.FactSales 
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
FROM `databricks-catalog`.bronze.retailsalesdata
