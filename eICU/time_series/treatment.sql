WITH
bg AS (
  SELECT * FROM (
    SELECT 
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , bg.labresultoffset - pairs.SaO2_timestamp AS delta_FiO2
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(bg.labresultoffset - pairs.SaO2_timestamp) ASC ) AS seq 
    , bg.labresult AS FiO2
    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN `datathon2023-team4.eicu_crd_us.lab` AS bg
    ON pairs.patientunitstayid = bg.patientunitstayid
    AND bg.labresultoffset <= pairs.SaO2_timestamp -- only looking at past values
    AND bg.labname = 'FiO2'
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)
, rrt AS (
    SELECT * FROM (
    SELECT 
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , rrt.treatmentoffset - pairs.SaO2_timestamp AS delta_rrt_tr
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(rrt.treatmentoffset - pairs.SaO2_timestamp) ASC ) AS seq 
    , rrt.treatmentstring AS rrt_path_tr
    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN `datathon2023-team4.eicu_crd_us.treatment` AS rrt
    ON pairs.patientunitstayid = rrt.patientunitstayid
    AND rrt.treatmentoffset <= pairs.SaO2_timestamp -- only looking at past values
    AND rrt.treatmentstring LIKE "%dialysis%"
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)
, io AS (
    SELECT * FROM (
    SELECT 
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , io.intakeoutputoffset - pairs.SaO2_timestamp AS delta_dialysis_total
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(io.intakeoutputoffset - pairs.SaO2_timestamp) ASC ) AS seq 
    , io.dialysistotal AS dialysis_total
    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN `datathon2023-team4.eicu_crd_us.intakeoutput` AS io
    ON pairs.patientunitstayid = io.patientunitstayid
    AND io.intakeoutputoffset <= pairs.SaO2_timestamp -- only looking at past values
    AND io.dialysistotal IS NOT NULL AND io.dialysistotal != 0
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)
, vt AS (
  SELECT patientunitstayid, airwaytype, ventstartoffset AS offset, 'start' AS event
  FROM `datathon2023-team4.eicu_crd_us.respiratorycare`
  WHERE ventstartoffset != 0

  UNION ALL

  -- invasiveventに関連する情報がある行を選択
  -- ventstartoffset < respcarestatusoffset なのでこちらが優先される
  SELECT patientunitstayid, airwaytype, respcarestatusoffset AS offset, 'start' AS event
  FROM `datathon2023-team4.eicu_crd_us.respiratorycare`
  WHERE ventstartoffset != 0
  AND ventendoffset = 0
  AND airwaytype in ('Oral ETT',
                     'Nasal ETT',
                     'Tracheostomy',
                     'Cricothyrotomy',
                     'Double-Lumen Tube')
  
  UNION ALL
  
  SELECT patientunitstayid, airwaytype, ventendoffset AS offset, 'end' AS event
  FROM `datathon2023-team4.eicu_crd_us.respiratorycare`
  WHERE ventendoffset != 0
  
  UNION ALL
  
  SELECT patientunitstayid, airwaytype, priorventstartoffset AS offset, 'start' AS event
  FROM `datathon2023-team4.eicu_crd_us.respiratorycare`
  WHERE priorventstartoffset != 0
  
  UNION ALL
  
  SELECT patientunitstayid, airwaytype, priorventendoffset AS offset, 'end' AS event
  FROM `datathon2023-team4.eicu_crd_us.respiratorycare`
  WHERE priorventendoffset != 0
)
, vt_unique AS (
   SELECT patientunitstayid, offset, any_value(airwaytype) as airwaytype, event
   FROM vt
   GROUP BY patientunitstayid, offset, event
)
, vent AS (
  SELECT * FROM
  (
    SELECT 
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , vent.offset - pairs.SaO2_timestamp AS delta_ventilation_status_offset
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(vent.offset - pairs.SaO2_timestamp) ASC ) AS seq 
    , CASE
        WHEN vent.event = 'start'
        THEN vent.airwaytype
        ELSE NULL
      END AS ventilation_status
    , CASE
        WHEN vent.event = 'start'
        THEN 1
        ELSE 0
      END AS ventilation_active
    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs
  
    LEFT JOIN vt_unique AS vent
    ON pairs.patientunitstayid = vent.patientunitstayid
    AND vent.offset <= pairs.SaO2_timestamp -- only looking at past values
  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)
,infusion AS (
  SELECT * FROM
  (
    SELECT
      pairs.patientunitstayid
    , pairs.SaO2_timestamp
    , CASE
        WHEN infusion.drugname IS NOT NULL
        THEN infusion.infusionoffset - pairs.SaO2_timestamp
        ELSE NULL
      END AS delta_vasopressin
    , ROW_NUMBER() OVER(PARTITION BY pairs.patientunitstayid, pairs.SaO2_timestamp
                        ORDER BY ABS(infusion.infusionoffset - pairs.SaO2_timestamp) ASC ) AS seq
    , infusion.drugname AS vasopressin_drugname

    FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

    LEFT JOIN `datathon2023-team4.eicu_crd_us.infusiondrug` infusion
    ON pairs.patientunitstayid = infusion.patientunitstayid
    AND infusion.infusionoffset <= pairs.SaO2_timestamp
    AND infusion.infusionoffset >= pairs.SaO2_timestamp - 60 -- 投与時間が60分前以降である
    AND (
      lower(drugname) LIKE "%epinephrine%"
      OR lower(drugname) LIKE "%norepinephrine%"
      OR lower(drugname) LIKE "%levophed%"
      OR lower(drugname) LIKE "%neosynephrine%"
      OR lower(drugname) LIKE "%neo-synephrine%"
      OR lower(drugname) LIKE "%phenylephrine%"
      OR lower(drugname) LIKE "%dopamine%"
      OR lower(drugname) LIKE "%vasopressin%"
      OR lower(drugname) LIKE "%dobutrex%"
      OR lower(drugname) LIKE "%milrinone%"
      OR lower(drugname) LIKE "%primacor%"
    )
    AND ( infusion.drugrate != "" 
          OR (infusion.infusionrate IS NOT NULL AND infusion.infusionrate > 0))

  )
  WHERE seq = 1
  ORDER BY patientunitstayid, SaO2_timestamp
)

SELECT
  pairs.patientunitstayid
, pairs.SaO2_timestamp

, CASE
    WHEN vent.ventilation_status IS NOT NULL
    THEN vent.delta_ventilation_status_offset
    ELSE NULL
  END AS delta_vent_start

, vent.ventilation_active
, vent.ventilation_status

, CASE
    WHEN vent.ventilation_status in ('Oral ETT',
                                      'Nasal ETT',
                                      'Tracheostomy',
                                      'Cricothyrotomy',
                                      'Double-Lumen Tube')
    THEN 1
    ELSE 0
  END AS invasive_vent

, CASE
    WHEN vent.ventilation_status IS NOT NULL
    THEN bg.delta_FiO2
    ELSE NULL
  END AS delta_FiO2

, CASE
    WHEN vent.ventilation_status IS NOT NULL
    THEN bg.FiO2
    ELSE NULL
  END AS FiO2

, CASE
    WHEN rrt.rrt_path_tr IS NOT NULL
    THEN rrt.delta_rrt_tr
    ELSE
      CASE
        WHEN io.dialysis_total IS NOT NULL
        THEN io.delta_dialysis_total
        ELSE NULL
      END
    END AS delta_rrt

, CASE
    WHEN ( rrt.rrt_path_tr IS NOT NULL
           OR io.dialysis_total IS NOT NULL)
    THEN 1
    ELSE 0
  END AS rrt

, infusion.delta_vasopressin AS delta_vp_start
, infusion.vasopressin_drugname AS vp_drugname
  

FROM `datathon2023-team4.eicu_crd_pulseOx_us.SaO2_SpO2_pairs` pairs

LEFT JOIN bg
ON pairs.patientunitstayid = bg.patientunitstayid
AND pairs.SaO2_timestamp = bg.SaO2_timestamp

LEFT JOIN rrt
ON pairs.patientunitstayid = rrt.patientunitstayid
AND pairs.SaO2_timestamp = rrt.SaO2_timestamp

LEFT JOIN io
ON pairs.patientunitstayid = io.patientunitstayid
AND pairs.SaO2_timestamp = io.SaO2_timestamp

LEFT JOIN vent
ON pairs.patientunitstayid = vent.patientunitstayid
AND pairs.SaO2_timestamp = vent.SaO2_timestamp

LEFT JOIN infusion
ON pairs.patientunitstayid = infusion.patientunitstayid
AND pairs.SaO2_timestamp = infusion.SaO2_timestamp

ORDER BY patientunitstayid, SaO2_timestamp ASC
