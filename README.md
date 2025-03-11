NYC Public Safety Big Data Analytics
This project leverages NYC Open Data to analyze public safety datasets including shooting incidents, stop-and-frisk (SQF), and use-of-force events in New York City. It demonstrates data ingestion, preprocessing, exploratory analysis, and machine learning modeling using Apache Spark and Python. The results are stored in Cloud SQL and visualizations are saved to Google Cloud Storage (GCS).

Table of Contents
Overview
Datasets
Project Structure
Preprocessing Pipelines
Analysis & Visualizations
Machine Learning Pipeline
Cloud Integration
Requirements
Setup and Execution
License
Overview
This project is designed to provide an end-to-end big data analytics pipeline that:

Ingests raw NYC public safety data from multiple sources.
Preprocesses the data using Apache Spark to handle missing values, standardize formats, and convert categorical fields.
Analyzes the cleaned data with various visualizations, including bar charts, line plots, and interactive maps to reveal trends by race, borough, and incident types.
Implements a machine learning pipeline (using a Decision Tree Classifier) to predict arrest outcomes based on stop-and-frisk data.
Integrates with cloud services by writing results to Cloud SQL and storing visualizations on Google Cloud Storage.
Datasets
The project utilizes datasets from NYC Open Data, including:

Shooting Incidents Data: Detailed historical records of shooting events.
Stop-and-Frisk (SQF) Data: Information from police stops, including arrest flags and demographic details.
Use of Force Data: Records detailing the use of force incidents, merged with subject information.
Project Structure
Inspired by the structure seen in NYC_Sentinel, the project is organized as follows:

graphql
Copy
├── data/
│   ├── NYPD_Shooting_Incident_Data__Historic__20240507.csv
│   ├── sqf-2023.xlsx
│   └── (other raw data files)
├── notebooks/
│   └── Use_of_Force.ipynb                # Jupyter notebook (analysis and exploration)
├── src/
│   ├── shooting_incidents_preprocess.py  # Data cleaning & transformation for shooting incidents
│   ├── shooting_incidents_analysis.py    # Exploratory data analysis for shooting incidents
│   ├── sqf_preprocess.py                 # Preprocessing for SQF data
│   ├── sqf_analysis.py                   # Data visualization and analysis for SQF
│   ├── sqf_ml.py                         # Machine learning pipeline for SQF data
│   ├── use_of_force_preprocess.py        # Data preprocessing for use of force incidents
│   └── use_of_force_analysis.py          # Analysis and visualization for use of force incidents
├── requirements.txt                      # List of dependencies (Spark, PySpark, matplotlib, folium, etc.)
└── README.md                             # This file
Each Python script is responsible for a specific step in the data pipeline—from ingesting and cleaning raw data, performing exploratory analysis, building and saving visualizations, to training and saving ML models.

Preprocessing Pipelines
Shooting Incidents
Script: shooting_incidents_preprocess.py
Tasks:
Reads CSV data.
Converts date strings into date objects.
Replaces various null representations.
Standardizes demographic fields (e.g., race, age, gender).
Writes the cleaned data to Cloud SQL.
Stop-and-Frisk (SQF)
Script: sqf_preprocess.py
Tasks:
Reads SQF data from an Excel file.
Cleans and standardizes categorical fields (e.g., borough names, race descriptions).
Converts boolean flags.
Writes the preprocessed SQF data to Cloud SQL.
Use of Force
Script: use_of_force_preprocess.py
Tasks:
Reads multiple CSV files (incidents and subjects).
Cleans data including handling of boroughs and subject demographic information.
Merges incident and subject details.
Writes the combined dataset to Cloud SQL.
Analysis & Visualizations
Shooting Incidents Analysis
Script: shooting_incidents_analysis.py
Visualizations:
Bar charts showing the number of incidents by perpetrator race and borough.
An interactive folium map with a Marker Cluster to display incident locations.
SQF Analysis
Script: sqf_analysis.py
Visualizations:
Time-series plots (line charts) showing trends in arrests and stops by month.
Bar charts to illustrate arrest distribution by borough.
Use of Force Analysis
Script: use_of_force_analysis.py
Visualizations:
Stacked bar charts to visualize the distribution of encounter basis across different force types.
Filtered visualizations highlighting trends by race (e.g., BLACK and HISPANIC groups) in specific encounter contexts.
Machine Learning Pipeline
SQF ML Model
Script: sqf_ml.py
Overview:
Feature Engineering: Converts categorical values using StringIndexer, imputes missing numerical values, and calculates a sentiment score for the demeanour of the person stopped using TextBlob.
Pipeline: Assembles features into a vector and trains a Decision Tree Classifier on a train-test split (70/30).
Output: The trained model is saved to Google Cloud Storage, making it available for further use or integration in production systems.
Cloud Integration
Database: Processed data from all modules are written to a Cloud SQL PostgreSQL instance.
Storage: Visualizations and the ML model are saved to a Google Cloud Storage bucket (gs://nyc-analytics/), ensuring scalability and easy sharing.
Big Data Processing: Apache Spark is used throughout for distributed data processing, enabling the handling of large datasets.
Requirements
Python 3.x
Apache Spark with PySpark
PostgreSQL and proper JDBC driver installed
Google Cloud SDK (for interacting with GCS and Cloud SQL)
Python libraries:
pandas
matplotlib
folium
pyspark
textblob
Other dependencies as listed in requirements.txt
Setup and Execution
Clone the Repository:

bash
Copy
git clone https://github.com/Phani-VarmaG/NYC_Sentinel.git
cd NYC_Sentinel
Install Dependencies:

Use pip to install required libraries:

bash
Copy
pip install -r requirements.txt
Configure Cloud Credentials:

Set up your Google Cloud credentials and update the JDBC connection properties (user, password, URL) in each script as needed.

Run the Preprocessing Scripts:

bash
Copy
spark-submit src/shooting_incidents_preprocess.py
spark-submit src/sqf_preprocess.py
spark-submit src/use_of_force_preprocess.py
Perform Data Analysis:

Run the analysis scripts to generate visualizations:

bash
Copy
spark-submit src/shooting_incidents_analysis.py
spark-submit src/sqf_analysis.py
spark-submit src/use_of_force_analysis.py
Run the Machine Learning Pipeline:

Train and save the model:

bash
Copy
spark-submit src/sqf_ml.py
View Visualizations:

Access your GCS bucket to view the saved plots and interactive maps.

License
This project is open-source and available under the MIT License.
