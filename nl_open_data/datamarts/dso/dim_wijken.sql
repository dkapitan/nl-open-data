CREATE OR REPLACE TABLE `dataverbinders.dso.dim_wijken_2018` AS (

  SELECT
    TRIM(kwb.Key) AS wijk_code
    , TRIM(kwb.Title) AS wijk_naam
  FROM `dataverbinders.cbs.84286NED_WijkenEnBuurten` AS kwb
  WHERE kwb.Key LIKE 'WK%'
  ORDER BY
    kwb.Key
)
