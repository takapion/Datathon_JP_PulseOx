WITH
hr AS (
  SELECT * FROM (
    SELECT
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , hr.observationoffset - pairs.SaO2_timestamp AS delta_heart_rate
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(hr.observationoffset - pairs.SaO2_timestamp) ASC ) as seq
    , hr.heartrate AS heart_rate

    FROM `eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN `eicu_crd_us.vitalperiodic` AS hr
    ON pairs.patientunitstayid = hr.patientunitstayid
    AND hr.heartrate IS NOT NULL
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)

,rr AS (
  SELECT * FROM (
    SELECT
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , rr.observationoffset - pairs.SaO2_timestamp AS delta_resp_rate
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(rr.observationoffset - pairs.SaO2_timestamp) ASC ) AS seq
    , rr.respiration AS resp_rate
  
    FROM `eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN `eicu_crd_us.vitalperiodic` AS rr
    ON pairs.patientunitstayid = rr.patientunitstayid
    AND rr.respiration IS NOT NULL
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)

, mbp AS (
  SELECT * FROM (
    SELECT
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , mbp.observationoffset - pairs.SaO2_timestamp AS delta_mbp
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(mbp.observationoffset - pairs.SaO2_timestamp) ASC ) AS seq
    , mbp.noninvasivemean AS mbp
  
    FROM `eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN `eicu_crd_us.vitalaperiodic` AS mbp
    ON pairs.patientunitstayid = mbp.patientunitstayid
    AND mbp.noninvasivemean IS NOT NULL
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)

, mbp_sys AS (
  SELECT * FROM (
    SELECT
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , mbp_sys.observationoffset - pairs.SaO2_timestamp AS delta_mbp_systemic
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(mbp_sys.observationoffset - pairs.SaO2_timestamp) ASC ) AS seq
    , mbp_sys.systemicmean AS mbp_systemic
  
    FROM `eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN `eicu_crd_us.vitalperiodic` AS mbp_sys
    ON pairs.patientunitstayid = mbp_sys.patientunitstayid
    AND mbp_sys.systemicmean IS NOT NULL
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)

, tmp  AS (
  SELECT * FROM (
    SELECT
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , tmp.observationoffset - pairs.SaO2_timestamp AS delta_temperature
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(tmp.observationoffset - pairs.SaO2_timestamp) ASC ) AS seq
    , tmp.temperature
  
    FROM `eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN `eicu_crd_us.vitalperiodic` AS tmp
    ON pairs.patientunitstayid = tmp.patientunitstayid
    AND tmp.temperature IS NOT NULL
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)

SELECT
  pairs.patientunitstayid
, pairs.SaO2_timestamp
, hr.delta_heart_rate
, hr.heart_rate
, mbp.delta_mbp
, mbp.mbp
, mbp_sys.delta_mbp_systemic
, mbp_sys.mbp_systemic
, rr.delta_resp_rate
, rr.resp_rate
, tmp.delta_temperature
, tmp.temperature

FROM `eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

LEFT JOIN hr
ON hr.patientunitstayid = pairs.patientunitstayid
AND hr.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN rr
ON rr.patientunitstayid = pairs.patientunitstayid
AND rr.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mbp
ON mbp.patientunitstayid = pairs.patientunitstayid
AND mbp.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN mbp_sys
ON mbp_sys.patientunitstayid = pairs.patientunitstayid
AND mbp_sys.SaO2_timestamp = pairs.SaO2_timestamp

LEFT JOIN tmp
ON tmp.patientunitstayid = pairs.patientunitstayid
AND tmp.SaO2_timestamp = pairs.SaO2_timestamp

ORDER BY patientunitstayid, SaO2_timestamp ASC