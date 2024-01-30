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
, patient.admissionweight AS weight
, patient.admissionheight AS height
, CASE
    WHEN patient.admissionheight > 0.1 THEN patient.admissionweight / (POWER(patient.admissionheight/100, 2))
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
, patient.hospitaladmitoffset
, patient.unitdischargeoffset
, patient.hospitaldischargeoffset
  -- from minutes to days
, CAST(CEILING( (patient.hospitaldischargeoffset - patient.hospitaladmitoffset) / 60 / 24 ) AS INT64) AS los_hospital
, (patient.unitdischargeoffset / 60 / 24) AS los_icu
, CASE
    WHEN patient.hospitaldischargestatus = "Expired" THEN 1
    ELSE 0
  END AS mortality_in
FROM `datathon2023-team4.eicu_crd_us.patient`
AS patient
