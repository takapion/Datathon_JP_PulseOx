-- enzyme Values include liver/abdominal markers from the lab
-- alt, alp, ast, bilirubin_total, bilirubin_direct, ck_cpk, ck_mb, ld_ldh

-- bilirubin_indirect does not exist in eICU
-- -- ggt missing

DROP TABLE IF EXISTS `datathon2023-team4.eicu_crd_pulseOx_us.enzyme`;
CREATE TABLE `datathon2023-team4.eicu_crd_pulseOx_us.enzyme` AS

WITH 

  alt AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , alt
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_alt
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND alt IS NOT NULL
  ) 
  WHERE seq = 1

)

, alp AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , alp
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_alp
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND alp IS NOT NULL
  ) 
  WHERE seq = 1

)

, ast AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ast
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_ast
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND ast IS NOT NULL

  ) 
  WHERE seq = 1

)

, bilirubin_total AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , bilirubin_total
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_bilirubin_total
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND bilirubin_total IS NOT NULL

  ) 
  WHERE seq = 1

)

, bilirubin_direct AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , bilirubin_direct
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_bilirubin_direct
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND bilirubin_direct IS NOT NULL

  ) 
  WHERE seq = 1

)

-- bilirubin_indirect missing

, ck_cpk AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ck_cpk
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_ck_cpk
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND ck_cpk IS NOT NULL

  ) 
  WHERE seq = 1

)

, ck_mb AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ck_mb AS ck_mb
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_ck_mb
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND ck_mb IS NOT NULL

  ) 
  WHERE seq = 1

)

-- ggt missing

, ld_ldh AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , ld_ldh
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_ld_ldh
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS enz
    ON enz.patientunitstayid = pairs.patientunitstayid
    AND ld_ldh IS NOT NULL

  ) 
  WHERE seq = 1

)


SELECT 
    pairs.patientunitstayid
  , pairs.SaO2_timestamp
  , alt.delta_alt
  , alt.alt
  , alp.delta_alp
  , alp.alp
  , ast.delta_ast
  , ast.ast
  , bilirubin_total.delta_bilirubin_total
  , bilirubin_total.bilirubin_total
  , bilirubin_direct.delta_bilirubin_direct
  , bilirubin_direct.bilirubin_direct
  , ck_cpk.delta_ck_cpk
  , ck_cpk.ck_cpk
  , ck_mb.delta_ck_mb
  , ck_mb.ck_mb
  , ld_ldh.delta_ld_ldh
  , ld_ldh.ld_ldh

FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

LEFT JOIN alt
ON alt.patientunitstayid = pairs.patientunitstayid
AND alt.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN alp
ON alp.patientunitstayid = pairs.patientunitstayid
AND alp.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ast
ON ast.patientunitstayid = pairs.patientunitstayid
AND ast.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bilirubin_total
ON bilirubin_total.patientunitstayid = pairs.patientunitstayid
AND bilirubin_total.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN bilirubin_direct
ON bilirubin_direct.patientunitstayid = pairs.patientunitstayid
AND bilirubin_direct.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ck_cpk
ON ck_cpk.patientunitstayid = pairs.patientunitstayid
AND ck_cpk.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ck_mb
ON ck_mb.patientunitstayid = pairs.patientunitstayid
AND ck_mb.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN ld_ldh
ON ld_ldh.patientunitstayid = pairs.patientunitstayid
AND ld_ldh.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY patientunitstayid, SaO2_timestamp
