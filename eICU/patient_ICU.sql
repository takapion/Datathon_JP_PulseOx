SELECT
  patient.patientunitstayid
, patient.patienthealthsystemstayid
, CASE
    WHEN patient.gender = "Female" THEN "F"
    WHEN patient.gender = "Male" THEN "M"
    ELSE NULL
  END AS gender
, CASE
    WHEN patient.gender = "Female" THEN 1 
    WHEN patient.gender = "Male" THEN 0
    ELSE NULL
  END AS sex_female
, patient.admissionWeight AS weight
, patient.admissionHeight AS height
, CASE
    WHEN patient.admissionHeight > 0.1 THEN patient.admissionWeight / (POWER(patient.admissionHeight/100, 2))
    ELSE NULL
  END AS BMI
, CASE
    WHEN patient.age = "> 89" THEN 90
    ELSE SAFE_CAST(patient.age AS INT64)
  END AS anchor_age
, patient.ethnicity
, CASE
    WHEN patient.ethnicity = "Caucasian" THEN "White"
    WHEN patient.ethnicity = "African American" THEN "Black"
    WHEN patient.ethnicity = "Hispanic" THEN "Hispanic"
    WHEN patient.ethnicity = "Asian" THEN "Asian"
    ELSE "Other"
  END AS race_group
, patient.hospitalAdmitOffset
, patient.unitDischargeOffset
, patient.hospitalDischargeOffset
, CAST(CEILING( (patient.hospitalDischargeOffset - patient.hospitalAdmitOffset) / 60 / 24 ) AS INT64) AS los_hospital
, (patient.unitDischargeOffset / 60 / 24) AS los_icu
, CASE
    WHEN patient.hospitalDischargeStatus = "Expired" THEN 1
    ELSE 0
  END AS mortality_in
FROM `datathon2023-team4.eicu_crd_us.patient`
AS patient
