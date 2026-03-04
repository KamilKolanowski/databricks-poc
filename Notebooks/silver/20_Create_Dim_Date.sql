-- Databricks notebook source
CREATE TABLE IF NOT EXISTS `databricks-catalog`.silver.DimDate (
  CalendarDateId INT,
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

INSERT OVERWRITE `databricks-catalog`.silver.DimDate
WITH date_range AS (
    SELECT EXPLODE(SEQUENCE(
        TO_DATE('2020-01-01'),
        TO_DATE('2030-12-31'),
        INTERVAL 1 DAY
    )) AS Date
)
SELECT
    CAST(DATE_FORMAT(Date, 'yyyyMMdd') AS INT)          AS CalendarDateId,
    Date                                                 AS CalendarDate,
    YEAR(Date)                                           AS FiscalYear,
    QUARTER(Date)                                        AS FiscalQuarter,
    MONTH(Date)                                          AS FiscalMonth,
    WEEKOFYEAR(Date)                                     AS FiscalWeek,
    DAYOFYEAR(Date)                                      AS FiscalDay,
    DATE_FORMAT(Date, 'EEEE')                            AS FiscalDayName,
    CONCAT('W', LPAD(WEEKOFYEAR(Date), 2, '0'))          AS FiscalWeekName,
    DATE_FORMAT(Date, 'MMMM')                            AS FiscalMonthName,
    CONCAT('Q', QUARTER(Date))                           AS FiscalQuarterName,
    CONCAT('FY', YEAR(Date))                             AS FiscalYearName,
    WEEKOFYEAR(Date)                                     AS FiscalWeekInYear,
    CEIL(DAYOFMONTH(Date) / 7.0)                         AS FiscalWeekInMonth,
    MONTH(Date) - ((QUARTER(Date) - 1) * 3)             AS FiscalMonthInQuarter,
    MONTH(Date)                                          AS FiscalMonthInYear,
    QUARTER(Date)                                        AS FiscalQuarterInYear
FROM date_range
