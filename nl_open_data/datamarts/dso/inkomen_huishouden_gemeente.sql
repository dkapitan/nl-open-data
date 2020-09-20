CREATE OR REPLACE TABLE `dataverbinders.dso.inkomen_huishouden_gemeente`
PARTITION BY RANGE_BUCKET(jaar, GENERATE_ARRAY(2011, 2020, 1))
AS (
  SELECT
    CAST(substr(fct.Perioden, 1, 4) AS INT64) AS jaar
    , regio.Key as gemeente_code
    , regio.Title as gemeente_naam
    , kvh.Key as huishouden_code
    , SUBSTR(kvh.Title, 7) as huishouden_omschrijving
    , fct.ParticuliereHuishoudens_1 as particuliere_huishoudens_aantal
    , fct.ParticuliereHuishoudensRelatief_2 as particuliere_huishoudens_relatief
    , fct.GemiddeldGestandaardiseerdInkomen_3 as inkomen_gestandaardiseerd_gemiddeld
    , fct.MediaanGestandaardiseerdInkomen_4 as inkomen_gestandaardiseerd_mediaan
    , fct.GemiddeldBesteedbaarInkomen_5 as inkomen_besteedbaar_gemiddeld
    , fct.MediaanBesteedbaarInkomen_6 as inkomen_besteedbaar_mediaan
    , fct.GestandaardiseerdInkomen1e10Groep_7 as inkomensgroep_deciel_1
    , fct.GestandaardiseerdInkomen2e10Groep_8 as inkomensgroep_deciel_2
    , fct.GestandaardiseerdInkomen3e10Groep_9 as inkomensgroep_deciel_3
    , fct.GestandaardiseerdInkomen4e10Groep_10 as inkomensgroep_deciel_4
    , fct.GestandaardiseerdInkomen5e10Groep_11 as inkomensgroep_deciel_5
    , fct.GestandaardiseerdInkomen6e10Groep_12 as inkomensgroep_deciel_6
    , fct.GestandaardiseerdInkomen7e10Groep_13 as inkomensgroep_deciel_7
    , fct.GestandaardiseerdInkomen8e10Groep_14 as inkomensgroep_deciel_8
    , fct.GestandaardiseerdInkomen9e10Groep_15 as inkomensgroep_deciel_9
    , fct.GestandaardiseerdInkomen10e10Groep_16 as inkomensgroep_deciel_10
  FROM `dataverbinders.cbs.84639NED_TypedDataSet` AS fct
  INNER JOIN `dataverbinders.cbs.84639NED_Regio` AS regio on regio.key = fct.Regio
  INNER JOIN `dataverbinders.cbs.84639NED_Populatie` AS pop ON pop.key = fct.Populatie
  INNER JOIN `dataverbinders.cbs.84639NED_KenmerkenVanHuishoudens` AS kvh ON kvh.key = fct.KenmerkenVanHuishoudens 
  WHERE
    fct.Regio LIKE 'GM%'
    AND fct.Populatie = '1050010'                 -- incl. studenten
    AND kvh.Title LIKE 'Type:%'                   -- uitsplitsing naar type huishouden
    AND fct.KenmerkenVanHuishoudens != '1050055'  -- geen subtotaal Type: Paar, totaal
)
