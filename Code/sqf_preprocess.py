from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, lit, to_date
import pandas as pd

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SQF Data Preprocessing") \
    .getOrCreate()

# Read Excel file using pandas
sqf = pd.read_excel("gs://datasets_nyc/sqf-2023.xlsx")  # Replace with your actual GCS bucket path

# Convert pandas DataFrame to PySpark DataFrame
sqf_data = spark.createDataFrame(sqf)

# Mapping dictionaries for data cleaning
borough_mapping = {
    "PBBX": "BRONX", "PBSI": "STATEN ISLAND", "PBMN": "MANHATTAN", "PBMS": "MANHATTAN",
    "PBBN": "BROOKLYN", "PBBS": "BROOKLYN", "PBQS": "QUEENS", "PBQN": "QUEENS",
    "STATEN IS": "STATEN ISLAND"
}
null_value_mapping = {
    "(null)": None, "NaN": None, "(": None, "NULL": None, "(nu": None, "#N/A": None
}
race_mapping = {
    "ASIAN / PACIFIC ISLANDER": "ASIAN/PACIFIC ISLANDER", "ASIAN/PAC.ISL": "ASIAN/PACIFIC ISLANDER",
    "AMER IND": "AMERICAN INDIAN/ALASKAN NATIVE", "AMERICAN INDIAN/ALASKAN N": "AMERICAN INDIAN/ALASKAN NATIVE",
    "MIDDLE EASTERN/SOUTHWEST": "MIDDLE EASTERN/SOUTHWEST ASIAN"
}
gender_mapping = {
    "M": "MALE", "F": "FEMALE"
}

# Applying mappings
for column, mapping in [("SUSPECT_RACE_DESCRIPTION", race_mapping), ("STOP_LOCATION_PATROL_BORO_NAME", borough_mapping)]:
    for key, value in mapping.items():
        sqf_data = sqf_data.withColumn(column, when(col(column) == key, value).otherwise(col(column)))

for column in sqf_data.columns:
    if column not in ["STOP_FRISK_DATE"]:  # Exclude date fields from null replacements
        for key, value in null_value_mapping.items():
            sqf_data = sqf_data.withColumn(column, when(col(column) == key, value).otherwise(col(column)))

from pyspark.sql.functions import when

# List of columns to convert
boolean_cols = ["FRISKED_FLAG", "SEARCHED_FLAG", "ASK_FOR_CONSENT_FLG", "CONSENT_GIVEN_FLG", 
                "OTHER_CONTRABAND_FLAG", "FIREARM_FLAG", "KNIFE_CUTTER_FLAG", "OTHER_WEAPON_FLAG", 
                "WEAPON_FOUND_FLAG", "PHYSICAL_FORCE_CEW_FLAG", "PHYSICAL_FORCE_DRAW_POINT_FIREARM_FLAG", 
                "PHYSICAL_FORCE_HANDCUFF_SUSPECT_FLAG", "PHYSICAL_FORCE_OC_SPRAY_USED_FLAG", 
                "PHYSICAL_FORCE_OTHER_FLAG", "PHYSICAL_FORCE_RESTRAINT_USED_FLAG", 
                "PHYSICAL_FORCE_VERBAL_INSTRUCTION_FLAG", "PHYSICAL_FORCE_WEAPON_IMPACT_FLAG", 
                "BACKROUND_CIRCUMSTANCES_VIOLENT_CRIME_FLAG", 
                "BACKROUND_CIRCUMSTANCES_SUSPECT_KNOWN_TO_CARRY_WEAPON_FLAG", 
                "SUSPECTS_ACTIONS_CASING_FLAG", "SUSPECTS_ACTIONS_CONCEALED_POSSESSION_WEAPON_FLAG", 
                "SUSPECTS_ACTIONS_DECRIPTION_FLAG", "SUSPECTS_ACTIONS_DRUG_TRANSACTIONS_FLAG", 
                "SUSPECTS_ACTIONS_IDENTIFY_CRIME_PATTERN_FLAG", "SUSPECTS_ACTIONS_LOOKOUT_FLAG", 
                "SUSPECTS_ACTIONS_OTHER_FLAG", "SUSPECTS_ACTIONS_PROXIMITY_TO_SCENE_FLAG", 
                "SEARCH_BASIS_ADMISSION_FLAG", "SEARCH_BASIS_CONSENT_FLAG", "SEARCH_BASIS_HARD_OBJECT_FLAG", 
                "SEARCH_BASIS_INCIDENTAL_TO_ARREST_FLAG", "SEARCH_BASIS_OTHER_FLAG", "SEARCH_BASIS_OUTLINE_FLAG"]

# Iterate over each column and apply transformation
for col_name in boolean_cols:
    sqf_data = sqf_data.withColumn(col_name, when(col(col_name) == "Y", 1).when(col(col_name).isNull() | (col(col_name) == "N"), 0))

# Define Cloud SQL connection properties
jdbc_url = "jdbc:postgresql://104.155.165.91:5432/SQF"
properties = {
    "user": "nyc-processed",
    "password": "BigData2024",
    "driver": "org.postgresql.Driver"
}

# Write data to Cloud SQL
sqf_data.write.jdbc(url=jdbc_url, table="SQF_Data   ", mode="append", properties=properties)

# Stop SparkSession
spark.stop()
