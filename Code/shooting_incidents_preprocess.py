from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date
import pandas as pd

# Start a Spark session
spark = SparkSession.builder \
    .appName("Shooting Incidents Preprocessing") \
    .getOrCreate()

# Read the Data from a CSV file (replace with your dataset's path)
shooting_data = spark.read.csv("NYPD_Shooting_Incident_Data__Historic__20240507.csv", header=True, inferSchema=True)

# Pre-processing steps
shooting_data = shooting_data.withColumn("OCCUR_DATE", when(col("OCCUR_DATE").isNotNull(), to_date(col("OCCUR_DATE"), 'MM/dd/yyyy')).otherwise(None))
null_value_mapping = {"(null)": None, "Nan": None, "NONE": None, "nan": None, "U": None, "UNKNOWN": None}
age_mapping = {"224": None, "1020": None, "940": None, "1022": None}
race_mapping = {"ASIAN / PACIFIC ISLANDER": "ASIAN/PACIFIC ISLANDER"}
gender_mapping = {"M": "MALE", "F": "FEMALE"}

# Applying mappings
for column, mapping in [("VIC_RACE", race_mapping), ("PERP_RACE", race_mapping), ("VIC_AGE_GROUP", age_mapping), ("PERP_AGE_GROUP", age_mapping), ("PERP_SEX", gender_mapping), ("VIC_SEX", gender_mapping)]:
    for key, value in mapping.items():
        shooting_data = shooting_data.withColumn(column, when(col(column) == key, value).otherwise(col(column)))

for column in shooting_data.columns:
    if column not in ["OCCUR_DATE"]:  # Exclude date fields from null replacements
        for key, value in null_value_mapping.items():
            shooting_data = shooting_data.withColumn(column, when(col(column) == key, value).otherwise(col(column)))

# Show processed data
shooting_data.show()

# Connect to the Cloud SQL database
jdbc_url = "jdbc:postgresql://104.155.165.91:5432/SQF"
properties = {
    "user": "nyc-processed",
    "password": "BigData2024",
    "driver": "org.postgresql.Driver"
}

# Write data to Cloud SQL
shooting_data.write.jdbc(url=jdbc_url, table="Shooting_Incidents", mode="append", properties=properties)

# Stop SparkSession
spark.stop()
