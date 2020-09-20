CREATE OR REPLACE TABLE `dataverbinders.dso.kerncijfers_wijken_en_buurten` AS (

  SELECT
    2018 AS jaar
    , wkbu.Municipality AS gemeente_code
    , TRIM(fct.Codering_3) AS wijk_code
    , fct.AantalInwoners_5 AS aantal_inwoners
    , fct.Mannen_6 AS aantal_mannen
    , fct.Vrouwen_7 AS aantal_vrouwen
    , fct.k_0Tot15Jaar_8 AS aantal_0_15_jaar
    , fct.k_15Tot25Jaar_9 AS aantal_15_25_jaar
    , fct.k_25Tot45Jaar_10 AS aantal_25_45_jaar
    , fct.k_45Tot65Jaar_11 AS aantal_45_65_jaar
    , fct.k_65JaarOfOuder_12 AS aantal_65_jaar_en_ouder
    , fct.Ongehuwd_13 AS aantal_ongehuwd
    , fct.Gehuwd_14 AS aantal_gehuwd
    , fct.Gescheiden_15 AS aantal_gescheiden
    , fct.Verweduwd_16 AS aantal_verweduwd
    , fct.WestersTotaal_17 AS aantal_westers
    , fct.NietWestersTotaal_18 AS aantal_niet_westers
    , fct.Marokko_19 AS aantal_marokaans
    , fct.NederlandseAntillenEnAruba_20 AS aantal_antillen
    , fct.Suriname_21 AS aantal_surinaams
    , fct.Turkije_22 AS aantal_turks
    , fct.OverigNietWesters_23 AS aantal_overige_niet_westers
    , fct.HuishoudensTotaal_28 AS aantal_huishoudens
    , fct.Eenpersoonshuishoudens_29 AS aantal_eenpersoons_huishoudens
    , fct.HuishoudensZonderKinderen_30 AS aantal_huishoudens_zonder_kinderen
    , fct.HuishoudensMetKinderen_31 AS aantal_huishoudens_met_kinderen
    , fct.Koopwoningen_40 AS aantal_koopwoningen
    , fct.AantalInkomensontvangers_64 AS aantal_inkomensontvangers
    , fct.GemiddeldInkomenPerInkomensontvanger_65 AS gemiddeld_inkomen_per_ontvanger
    , fct.GemiddeldInkomenPerInwoner_66 AS gemiddeld_inkomen_per_inwoner
    , fct.PersonenautoSTotaal_89 AS aantal_personenautos
    , fct.PersonenPerSoortUitkeringBijstand_74 AS aantal_personen_bijstand
    , fct.PersonenPerSoortUitkeringAO_75 AS aantal_personen_ao
    , fct.PersonenPerSoortUitkeringWW_76 AS aantal_personen_ww
    , fct.PersonenPerSoortUitkeringAOW_77 AS aantal_personen_aow
  FROM `dataverbinders.cbs.84286NED_TypedDataSet` AS fct
  INNER JOIN `dataverbinders.cbs.84286NED_WijkenEnBuurten` AS wkbu ON wkbu.key = fct.Codering_3 
  WHERE
    fct.Codering_3 LIKE 'WK%'
  ORDER BY
    gemeente_code
    , wijk_code
  
)
