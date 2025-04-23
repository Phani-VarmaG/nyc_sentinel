# Overview

This project leverages NYC Open Data to analyze public safety datasets
including shooting incidents, stop-and-frisk (SQF), and use-of-force
events in New York City. It demonstrates data ingestion, preprocessing,
exploratory analysis, and machine learning modeling using Apache Spark
and Python. The results are stored in Cloud SQL and visualizations are
saved to Google Cloud Storage (GCS).

# Datasets

The project utilizes datasets from NYC Open Data, including:

-   **Shooting Incidents Data:** Detailed historical records of shooting
    events.

-   **Stop-and-Frisk (SQF) Data:** Information from police stops,
    including arrest flags and demographic details.

-   **Use of Force Data:** Records detailing use-of-force incidents,
    merged with subject information.

# Project Structure

The project is organized as follows:

    ├── data/
    │   ├── NYPD_Shooting_Incident_Data__Historic__20240507.csv
    │   ├── sqf-2023.xlsx
    │   └── (other raw data files)
    ├── notebooks/
    │   └── Use_of_Force.ipynb                % Jupyter notebook (analysis and exploration)
    ├── src/
    │   ├── shooting_incidents_preprocess.py  % Data cleaning & transformation for shooting incidents
    │   ├── shooting_incidents_analysis.py    % Exploratory data analysis for shooting incidents
    │   ├── sqf_preprocess.py                 % Preprocessing for SQF data
    │   ├── sqf_analysis.py                   % Data visualization and analysis for SQF
    │   ├── sqf_ml.py                         % Machine learning pipeline for SQF data
    │   ├── use_of_force_preprocess.py        % Data preprocessing for use of force incidents
    │   └── use_of_force_analysis.py          % Analysis and visualization for use of force incidents
    ├── requirements.txt                      % List of dependencies
    └── README.md                             % This file

Each Python script is responsible for a specific step in the data
pipeline---from ingesting and cleaning raw data, performing exploratory
analysis, building and saving visualizations, to training and saving
machine learning models.

# Preprocessing Pipelines

## Shooting Incidents

-   **Script:** `shooting_incidents_preprocess.py`

-   **Tasks:**

    -   Reads CSV data.

    -   Converts date strings into date objects.

    -   Replaces various null representations.

    -   Standardizes demographic fields (e.g., converting race strings
        to a unified format).

    -   Writes the cleaned data to Cloud SQL.

## Stop-and-Frisk (SQF)

-   **Script:** `sqf_preprocess.py`

-   **Tasks:**

    -   Reads SQF data from an Excel file.

    -   Cleans and standardizes categorical fields (e.g., borough names,
        race descriptions).

    -   Converts boolean flags.

    -   Writes the preprocessed SQF data to Cloud SQL.

## Use of Force

-   **Script:** `use_of_force_preprocess.py`

-   **Tasks:**

    -   Reads multiple CSV files (incidents and subjects).

    -   Cleans data including handling of boroughs and subject
        demographic information.

    -   Merges incident and subject details.

    -   Writes the combined dataset to Cloud SQL.

# Analysis & Visualizations

## Shooting Incidents Analysis

-   **Script:** `shooting_incidents_analysis.py`

-   **Visualizations:**

    -   Bar charts showing the number of incidents by perpetrator race
        and borough.

    -   An interactive map using the `folium` library with marker
        clustering to display incident locations.

## SQF Analysis

-   **Script:** `sqf_analysis.py`

-   **Visualizations:**

    -   Time-series plots (line charts) showing trends in arrests and
        stops by month.

    -   Bar charts illustrating the arrest distribution by borough.

## Use of Force Analysis

-   **Script:** `use_of_force_analysis.py`

-   **Visualizations:**

    -   Stacked bar charts visualizing the distribution of encounter
        basis across different force types.

    -   Filtered visualizations highlighting trends by race (e.g., BLACK
        and HISPANIC groups) in specific encounter contexts.

# Machine Learning Pipeline

## SQF ML Model

-   **Script:** `sqf_ml.py`

-   **Overview:**

    -   **Feature Engineering:** Converts categorical values using
        `StringIndexer`, imputes missing numerical values, and
        calculates a sentiment score for the demeanour using TextBlob.

    -   **Pipeline:** Assembles features into a single vector and trains
        a Decision Tree Classifier on a 70/30 train-test split.

    -   **Output:** The trained model is saved to Google Cloud Storage,
        making it available for further use or integration in production
        systems.

# Cloud Integration

-   **Database:** Processed data from all modules is written to a Cloud
    SQL PostgreSQL instance.

-   **Storage:** Visualizations and the ML model are saved to a Google
    Cloud Storage bucket (e.g., `gs://nyc-analytics/`), ensuring
    scalability and easy sharing.

-   **Big Data Processing:** Apache Spark is used throughout the project
    for distributed data processing, enabling the handling of large
    datasets.

# Requirements

-   **Python 3.x**

-   **Apache Spark** with PySpark

-   **PostgreSQL** and the proper JDBC driver

-   **Google Cloud SDK** (for interacting with GCS and Cloud SQL)

-   **Python Libraries:**

    -   `pandas`

    -   `matplotlib`

    -   `folium`

    -   `pyspark`

    -   `textblob`

    -   Other dependencies as listed in `requirements.txt`

# Setup and Execution

## 1. Clone the Repository

    git clone https://github.com/Phani-VarmaG/NYC_Sentinel.git
    cd NYC_Sentinel

## 2. Install Dependencies

Use `pip` to install the required libraries:

    pip install -r requirements.txt

## 3. Configure Cloud Credentials

Set up your Google Cloud credentials and update the JDBC connection
properties (user, password, URL) in each script as needed.

## 4. Run the Preprocessing Scripts

    spark-submit src/shooting_incidents_preprocess.py
    spark-submit src/sqf_preprocess.py
    spark-submit src/use_of_force_preprocess.py

## 5. Perform Data Analysis

Run the analysis scripts to generate visualizations:

    spark-submit src/shooting_incidents_analysis.py
    spark-submit src/sqf_analysis.py
    spark-submit src/use_of_force_analysis.py

## 6. Run the Machine Learning Pipeline

Train and save the model:

    spark-submit src/sqf_ml.py

## 7. View Visualizations

Access your GCS bucket to view the saved plots and interactive maps.

# License

This project is open-source and available under the [MIT
License](https://opensource.org/licenses/MIT).
