-- Databricks notebook source
CREATE TABLE IF NOT EXISTS `databricks-catalog`.gold.FactSales (
  TransactionId INT NOT NULL,
  CalendarDateId INT NOT NULL,
  CustomerId BIGINT NOT NULL,
  ProductCategoryId BIGINT NOT NULL,
  Quantity INT NOT NULL,
  UnitPrice INT NOT NULL,
  TotalAmount INT NOT NULL,
  PRIMARY KEY (TransactionId)
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW SilverDateRange AS
SELECT dd.CalendarDateId
FROM `databricks-catalog`.gold.DimDate AS dd
INNER JOIN `databricks-catalog`.silver.FactSales AS sf
    ON dd.CalendarDate = sf.Date;


-- COMMAND ----------

DELETE FROM `databricks-catalog`.gold.FactSales
WHERE CalendarDateId IN (
    SELECT CalendarDateId FROM SilverDateRange
);


-- COMMAND ----------

INSERT INTO `databricks-catalog`.gold.FactSales
SELECT
    f.TransactionId,
    dd.CalendarDateId,
    dc.CustomerId,
    dp.ProductCategoryId,
    f.Quantity,
    f.UnitPrice,
    f.TotalAmount
FROM `databricks-catalog`.silver.FactSales AS f
INNER JOIN `databricks-catalog`.gold.DimDate AS dd
    ON f.Date = dd.CalendarDate
INNER JOIN `databricks-catalog`.gold.DimCustomer AS dc
    ON f.CustomerId = dc.CustomerSourceId
    AND dc.IsCurrent = true
INNER JOIN `databricks-catalog`.gold.DimProductCategory AS dp
    ON f.ProductCategory = dp.ProductCategory;
