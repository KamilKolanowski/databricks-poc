-- Databricks notebook source
CREATE TABLE IF NOT EXISTS `databricks-catalog`.gold.DimDate (
  CalendarDateId INT NOT NULL,
  CalendarDate DATE,
  FiscalYear INT,
  FiscalQuarter INT,
  FiscalMonth INT,
  FiscalWeek INT,
  FiscalDay INT,
  FiscalDayName STRING,
  FiscalWeekName STRING,
  FiscalMonthName STRING,
  FiscalQuarterName STRING,
  FiscalYearName STRING,
  FiscalWeekInYear INT,
  FiscalWeekInMonth INT,
  FiscalMonthInQuarter INT,
  FiscalMonthInYear INT,
  FiscalQuarterInYear INT,
  PRIMARY KEY (CalendarDateId)
)


-- COMMAND ----------

MERGE INTO `databricks-catalog`.gold.DimDate AS target
USING `databricks-catalog`.silver.DimDate AS source
    ON target.CalendarDateId = source.CalendarDateId
WHEN NOT MATCHED 
    THEN INSERT *
