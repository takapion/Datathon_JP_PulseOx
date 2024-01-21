-- Limiting variable: SaO2
-- We fetch all possible SaO2. Each of these is aligned with the closest SpO2 value, in a 90min window, and record the delta

DROP TABLE IF EXISTS `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs`;
CREATE TABLE `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` AS

-- Auxiliary to fetch the SaO2 timestamps and get the corresponding SpO2
WITH pairs AS (
  SELECT
    sao2_table.patientunitstayid
  , sao2_table.labresultoffset AS SaO2_timestamp
  , MAX(spo2_table.observationoffset) AS SpO2_timestamp

  FROM `datathon2023-team4.eicu_crd_us.lab` AS sao2_table

  LEFT JOIN(
    SELECT 
      patientunitstayid
    , observationoffset
    , CASE WHEN sao2 > 0 AND sao2 <= 100 THEN sao2 ELSE NULL END AS SpO2
    FROM `datathon2023-team4.eicu_crd_us.vitalperiodic` spo2_table
    WHERE sao2 IS NOT NULL
  )
  AS spo2_table
  ON spo2_table.patientunitstayid = sao2_table.patientunitstayid
  -- each ABG-measured sao2_table is matched with the closest SpO2 value recorded within the previous 90 minutes"
  AND sao2_table.labresultoffset - spo2_table.observationoffset <= 5
  AND sao2_table.labresultoffset - spo2_table.observationoffset >= 0

  -- Only for SaO2 values
  WHERE sao2_table.labname = 'O2 Sat (%)' -- SaO2
  GROUP BY patientunitstayid, SaO2_timestamp
  ORDER BY SaO2_timestamp ASC
)

SELECT
    pairs.patientunitstayid
  , SaO2_timestamp
  , sao2_vals.labresult AS SaO2
  , SpO2_timestamp - SaO2_timestamp AS delta_SpO2
  , spo2_vals.sao2 AS SpO2
  , CASE WHEN spo2_vals.sao2 >= 88 AND sao2_vals.labresult < 88 THEN 1 ELSE 0 END AS hidden_hypoxemia

FROM pairs

LEFT JOIN `datathon2023-team4.eicu_crd_us.lab`
AS sao2_vals
ON sao2_vals.patientunitstayid = pairs.patientunitstayid
AND sao2_vals.labresultoffset = pairs.SaO2_timestamp
AND sao2_vals.labname = 'O2 Sat (%)'

LEFT JOIN `datathon2023-team4.eicu_crd_us.vitalperiodic`
AS spo2_vals
ON spo2_vals.patientunitstayid = pairs.patientunitstayid
AND spo2_vals.observationoffset = pairs.SpO2_timestamp

WHERE spo2_vals.sao2 IS NOT NULL
  AND sao2_vals.labresult IS NOT NULL

ORDER BY patientunitstayid, SaO2_timestamp ASC
