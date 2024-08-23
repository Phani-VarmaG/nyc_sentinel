from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Use of Force Incidents Preprocessing") \
    .getOrCreate()

# Read CSV files
use_of_force_incidents = spark.read.csv("gs://datasets_nyc/NYPD_Use_of_Force_Incidents.csv", header=True, inferSchema=True)
use_of_force_subjects = spark.read.csv("gs://datasets_nyc/NYPD_Use_of_Force__Subjects.csv", header=True, inferSchema=True)

# Perform data preprocessing for use_of_force_incidents
use_of_force_incidents = use_of_force_incidents.drop("YEARMONTHSHORT")
use_of_force_incidents = use_of_force_incidents.filter(col("PATROL BOROUGH").isNotNull())

borough_mapping = {
    "PBBX": "BRONX", "PBSI": "STATEN ISLAND", "PBMN": "MANHATTAN", "PBMS": "MANHATTAN",
    "PBBN": "BROOKLYN", "PBBS": "BROOKLYN", "PBQS": "QUEENS", "PBQN": "QUEENS"
}

mapping_expr = when(col("PATROL BOROUGH") == 'PBBX', 'BRONX')
for key, value in borough_mapping.items():
    mapping_expr = mapping_expr.when(col("PATROL BOROUGH") == key, value)

use_of_force_incidents = use_of_force_incidents.withColumn("PATROL BOROUGH", mapping_expr.otherwise(col("PATROL BOROUGH")))

# Perform data preprocessing for use_of_force_subjects
use_of_force_subjects = use_of_force_subjects.withColumn("SUBJECT GENDER", when(col("SUBJECT GENDER") == "UNK", None).otherwise(col("SUBJECT GENDER")))
use_of_force_subjects = use_of_force_subjects.withColumn(
    "SUBJECT RACE",
    when(col("SUBJECT RACE") == "ASIAN", "ASIAN/PACIFIC ISLANDER")
    .when(col("SUBJECT RACE") == "AMER INDIAN", "AMERICAN INDIAN/ALASKAN NATIVE")
    .otherwise(col("SUBJECT RACE"))
)
use_of_force_subjects = use_of_force_subjects.drop("SUBJECT INJURED", "SUBJECT INJURY LEVEL", "SUBJECT USED FORCE")

# Merge the use_of_force_subjects DataFrame into use_of_force_incidents
use_of_force_combined = use_of_force_incidents.join(
    use_of_force_subjects,
    use_of_force_incidents["TRI INCIDENT NUMBER"] == use_of_force_subjects["TRI INCIDENT NUMBER"],
    how="inner"
)

# Drop the duplicate 'TRI INCIDENT NUMBER' column if it exists
if "TRI Incident Number" in use_of_force_combined.columns:
    use_of_force_combined = use_of_force_combined.drop("TRI Incident Number")

# Connect to the Cloud SQL database
jdbc_url = "jdbc:postgresql://104.155.165.91:5432/SQF"
properties = {
    "user": "nyc-processed",
    "password": "BigData2024",
    "driver": "org.postgresql.Driver"
}

# Write the preprocessed data to the database
use_of_force_combined.write.jdbc(url=jdbc_url, table="Use_of_Force", mode="overwrite", properties=properties)

# Stop SparkSession
spark.stop()
