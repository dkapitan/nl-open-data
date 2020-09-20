CREATE OR REPLACE TABLE `dataverbinders.dso.gebruik_wmo_pgb_gemeente`
PARTITION BY RANGE_BUCKET(jaar, GENERATE_ARRAY(2011, 2020, 1)) AS (

  SELECT
    CAST(substr(fct.Perioden, 1, 4) AS INT64) as jaar
    , RegioS.Key as gemeente_code
    , RegioS.Title as gemeente_naam
    , Geslacht.Title as geslacht
    , Leeftijd.Title as leefijd
    , Huishouden.Key as huishouden_code
    , Huishouden.Title as huishouden_omschrijving
    , Type.Title as voorziening
    , LeveringsvormZorg.Title as leveringsvorm
    , fct.PersonenMetGebruikInJaar_1 as gebruik_aantal_personen
    , fct.PersonenMetGebruikInJaar_2 as gebruik_percentage
  FROM `dataverbinders.mlz.40060NED_TypedDataSet` AS fct
  INNER JOIN `dataverbinders.mlz.40060NED_Perioden` AS Perioden ON Perioden.key = fct.Perioden
  INNER JOIN `dataverbinders.mlz.40060NED_Geslacht` AS Geslacht ON Geslacht.key = fct.Geslacht
  INNER JOIN `dataverbinders.mlz.40060NED_Leeftijd` AS Leeftijd ON Leeftijd.key = fct.Leeftijd
  INNER JOIN `dataverbinders.mlz.40060NED_Huishouden` AS Huishouden on Huishouden.key = fct.Huishouden
  INNER JOIN `dataverbinders.mlz.40060NED_TypeMaatwerkvoorziening`  AS Type on Type.key = fct.TypeMaatwerkvoorziening
  INNER JOIN `dataverbinders.mlz.40060NED_LeveringsvormZorg` AS LeveringsvormZorg on LeveringsvormZorg.key = fct.LeveringsvormZorg
  INNER JOIN `dataverbinders.mlz.40060NED_RegioS` AS RegioS on RegioS.key = fct.RegioS
  WHERE
    fct.Perioden LIKE '%JJ%'
    AND fct.RegioS LIKE 'GM%'
    AND fct.Geslacht != 'T001038'
    AND fct.Leeftijd NOT IN ('20300', '90120', '90200')
    AND fct.Huishouden != 'T001139'
    AND fct.TypeMaatwerkvoorziening != 'T001024'
    AND fct.LeveringsvormZorg != 'T001306'
)
