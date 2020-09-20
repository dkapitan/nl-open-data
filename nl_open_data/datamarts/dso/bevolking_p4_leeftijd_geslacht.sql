DROP TABLE IF EXISTS dataverbinders.dso.bevolking_p4_leeftijd_geslacht;

CREATE TABLE dataverbinders.dso.bevolking_p4_leeftijd_geslacht
PARTITION BY RANGE_BUCKET(jaar, GENERATE_ARRAY(1999, 2030, 1))
AS (
  SELECT
    cast(substr(fct.Perioden, 1, 4) as INT64) AS jaar,
    pc.Title AS pc4,
    gsl.Title AS geslacht,
    lft.Title AS leeftijd,
    CAST(fct.Bevolking_1 AS INT64) AS bevolking
  FROM `dataverbinders.cbs.83502NED_TypedDataSet` AS fct
  LEFT JOIN `dataverbinders.cbs.83502NED_Postcode` AS pc ON pc.Key = fct.Postcode 
  LEFT JOIN `dataverbinders.cbs.83502NED_Geslacht` AS gsl ON gsl.Key = fct.Geslacht
  LEFT JOIN `dataverbinders.cbs.83502NED_Leeftijd` AS lft ON lft.Key = fct.Leeftijd
  WHERE
    pc.Key != 'NL01'
    AND gsl.Key != 'T001038'
    AND lft.Key != '10000'
);
