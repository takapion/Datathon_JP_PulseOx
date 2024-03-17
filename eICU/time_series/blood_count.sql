-- Blood Values include hemoglobin, hematocrit, mch, mchc, mcv, platelets, rbc, rdw, wbc

DROP TABLE IF EXISTS `datathon2023-team4.eicu_crd_pulseOx_us.blood_count`;
CREATE TABLE `datathon2023-team4.eicu_crd_pulseOx_us.blood_count` AS

-- Blood Values include hemoglobin, hematocrit, mch, mchc, mcv, platelets, rbc, rdw, wbc

WITH 
  hemoglobin AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , hemoglobin
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_hemoglobin
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND hemoglobin IS NOT NULL

  ) 
  WHERE seq = 1

)

, hematocrit AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , hematocrit
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_hematocrit
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND hematocrit IS NOT NULL

  ) 
  WHERE seq = 1

)

, mch AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , mch
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_mch
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND mch IS NOT NULL

  ) 
  WHERE seq = 1

)

, mchc AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , mchc
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_mchc
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND mchc IS NOT NULL

  ) 
  WHERE seq = 1

)

, mcv AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , mcv
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_mcv
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND mcv IS NOT NULL

  ) 
  WHERE seq = 1

)

, platelets AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , platelets
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_platelets
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND platelets IS NOT NULL

  ) 
  WHERE seq = 1

)

, rbc AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , rbc
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_rbc
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND rbc IS NOT NULL

  ) 
  WHERE seq = 1

)

, rdw AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , rdw
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_rdw
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND rdw IS NOT NULL

  ) 
  WHERE seq = 1
  
)

, wbc AS (
  SELECT * FROM(
    SELECT
      pairs.patientunitstayid
    , wbc
    , pairs.SaO2_timestamp
    , (chartoffset - SaO2_timestamp) AS delta_wbc
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(chartoffset - SaO2_timestamp) ASC) AS seq

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_pulseOx_us.pivoted_lab2`
    AS blood_counts
    ON blood_counts.patientunitstayid = pairs.patientunitstayid
    AND wbc IS NOT NULL


  ) 
  WHERE seq = 1

)

SELECT 
    pairs.patientunitstayid
  , pairs.SaO2_timestamp
  , hemoglobin.delta_hemoglobin
  , hemoglobin.hemoglobin
  , hematocrit.delta_hematocrit
  , hematocrit.hematocrit
  , mch.delta_mch
  , mch.mch
  , mchc.delta_mchc
  , mchc.mchc
  , mcv.delta_mcv
  , mcv.mcv
  , platelets.delta_platelets
  , platelets.platelets
  , rbc.delta_rbc
  , rbc.rbc
  , rdw.delta_rdw
  , rdw.rdw
  , wbc.delta_wbc
  , wbc.wbc

FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

LEFT JOIN hemoglobin
ON hemoglobin.patientunitstayid = pairs.patientunitstayid
AND hemoglobin.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN hematocrit
ON hematocrit.patientunitstayid = pairs.patientunitstayid
AND hematocrit.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mch
ON mch.patientunitstayid = pairs.patientunitstayid
AND mch.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mchc
ON mchc.patientunitstayid = pairs.patientunitstayid
AND mchc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mcv
ON mcv.patientunitstayid = pairs.patientunitstayid
AND mcv.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN platelets
ON platelets.patientunitstayid = pairs.patientunitstayid
AND platelets.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rbc
ON rbc.patientunitstayid = pairs.patientunitstayid
AND rbc.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rdw
ON rdw.patientunitstayid = pairs.patientunitstayid
AND rdw.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN wbc
ON wbc.patientunitstayid = pairs.patientunitstayid
AND wbc.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY patientunitstayid, SaO2_timestamp
