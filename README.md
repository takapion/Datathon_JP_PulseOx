# SaO2-SpO2 Gap Prediction in ICU Patients

## Overview
This project aims to predict the gap between arterial oxygen saturation (SaO2) and pulse oximetry saturation (SpO2) in ICU patients using data from the MIMIC-IV and eICU databases. This work was initiated as part of Datathon Japan, September 2023. Our goal is to enhance understanding and prediction of oxygenation discrepancies in critical care, aiding in better patient management.

## Databases
The project utilizes two critical care databases:

- MIMIC-IV: A large database comprising de-identified health-related data associated with over forty thousand patients who stayed in critical care units of the Beth Israel Deaconess Medical Center between 2001 and 2012.
- eICU: A multi-center database comprising over 200,000 admissions to intensive care units across the United States between 2014 and 2015.

## Repository Structure
```
├── MIMIC-IV
│   └── sql_code.sql           # SQL code for extracting relevant patients from MIMIC-IV
├── eICU
│   └── sql_code.sql           # SQL code for extracting relevant patients from eICU
└── Analysis
    └── model_prediction.py    # Python code for building and evaluating the prediction model
```

## Getting Started
To use this repository, follow these steps:

1. Clone the Repository: Clone this repository to your local machine.
2. Database Setup: Ensure you have access to MIMIC-IV and eICU databases. Follow their respective guidelines for access and setup.
3. Data Extraction: Run the SQL scripts in MIMIC-IV and eICU folders to extract the relevant patient data.
4. Data Analysis: Navigate to the Analysis folder and run the model_prediction.py script to build and evaluate the prediction model.

## Requirements
- Python 3.8 or higher
- SQL Server (for running SQL scripts)
- Required Python libraries: pandas, numpy, scikit-learn, etc. (See requirements.txt for a full list)
## Contributing
We welcome contributions from the community. Please read our contributing guidelines before submitting pull requests.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements
- Datathon Japan September 2023
- Contributors and participants of the project
- MIMIC-IV and eICU database teams for providing the data

## Contact
For any queries, please contact us at [takahiro.kinoshita@medicu.co.jp].
