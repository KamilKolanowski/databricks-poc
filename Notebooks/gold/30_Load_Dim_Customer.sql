-- Databricks notebook source
-- DBTITLE 1,CREATE TABLE DimCustomer (fixed)
CREATE TABLE IF NOT EXISTS `databricks-catalog`.gold.DimCustomer (
  CustomerId BIGINT GENERATED ALWAYS AS IDENTITY,
  CustomerSourceId STRING NOT NULL, 
  Gender STRING NOT NULL, 
  Age INT NOT NULL, 
  ValidFrom TIMESTAMP NOT NULL,
  ValidTo TIMESTAMP,
  IsCurrent BOOLEAN NOT NULL,
  PRIMARY KEY (CustomerId)
)


-- COMMAND ----------

MERGE INTO `databricks-catalog`.gold.DimCustomer AS target
USING `databricks-catalog`.silver.DimCustomer AS source
    ON target.CustomerSourceId = source.CustomerSourceId
    AND target.IsCurrent = true
WHEN MATCHED AND (target.Gender != source.Gender
              OR target.Age != source.Age)
  THEN UPDATE SET
    target.ValidTo = CURRENT_TIMESTAMP() - INTERVAL 1 DAY,
    target.IsCurrent = false;
  
MERGE INTO `databricks-catalog`.gold.DimCustomer AS target
USING `databricks-catalog`.silver.DimCustomer AS source
  ON target.CustomerSourceId = source.CustomerSourceId
    AND target.IsCurrent = true
WHEN NOT MATCHED THEN INSERT (CustomerSourceId, Gender, Age, ValidFrom, ValidTo, IsCurrent)
  VALUES (source.CustomerSourceId, source.Gender, source.Age, CURRENT_TIMESTAMP(), NULL, true)
