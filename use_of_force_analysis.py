import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("NYC Use of Force Analysis") \
    .getOrCreate()

# Connect to the Cloud SQL database
jdbc_url = "jdbc:postgresql://104.155.165.91:5432/SQF"
properties = {
    "user": "nyc-processed",
    "password": "BigData2024",
    "driver": "org.postgresql.Driver"
}

# Read data from the database
use_of_force_combined = spark.read.jdbc(url=jdbc_url, table="Use_of_Force", properties=properties)

# Plot 1: Distribution of Basis for Encounter across Different Force Types

# Group by 'ForceType' and 'BasisForEncounter', count occurrences
grouped_df = use_of_force_combined.groupBy('ForceType', 'BasisForEncounter').count().orderBy('ForceType')

# Pivot the DataFrame
pivot_df = grouped_df.groupBy('ForceType').pivot('BasisForEncounter').sum('count').fillna(0)

# Convert PySpark DataFrame to Pandas DataFrame for plotting
pivot_df_pd = pivot_df.toPandas()

# Customizing colors
colors = plt.cm.tab20.colors[:len(pivot_df_pd.columns)-1]  # Selecting colors from a colormap

# Plotting
pivot_df_pd.plot(kind='bar', stacked=True, figsize=(12, 8), color=colors)
plt.title('Distribution of Basis for Encounter across Different Force Types')
plt.xlabel('Force Type')
plt.ylabel('Count')
plt.xticks(range(len(pivot_df_pd['ForceType'])), pivot_df_pd['ForceType'])
plt.legend(title='Basis For Encounter', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig('gs://nyc-analytics/force_type_basis_encounter_distribution.png')  # Save plot to Cloud Storage
plt.close()

# Plot 2: Count of Suspicious Activity encounters by Force Type

# Filter the DataFrame to include only rows with 'BasisForEncounter' as 'Suspicious Activity'
suspicious_activity_df = use_of_force_combined.filter(use_of_force_combined['BasisForEncounter'] == 'SUSPICIOUS ACTIVITY')

# Group by 'ForceType' and count occurrences
force_type_counts = suspicious_activity_df.groupBy('ForceType').count().orderBy('count')

# Convert PySpark DataFrame to Pandas DataFrame for plotting
force_type_counts_pd = force_type_counts.toPandas()

# Plotting
plt.figure(figsize=(10, 6))
plt.barh(force_type_counts_pd['ForceType'], force_type_counts_pd['count'], color='yellow')
plt.xlabel('Count')
plt.ylabel('Force Type')
plt.title('Count of Suspicious Activity encounters by Force Type')
plt.tight_layout()
plt.savefig('gs://nyc-analytics/suspicious_activity_encounters_by_force_type.png')  # Save plot to Cloud Storage
plt.close()

# Plot 3: Count of CRIME/VIOLATION IN PROGRESS encounters by Force Type for BLACK race

# Filter the DataFrame for race type as 'BLACK' and basis of occurrence as 'SUSPICIOUS ACTIVITY'
filtered_df = use_of_force_combined.filter((col('SUBJECT RACE') == 'BLACK') & (col('BasisForEncounter') == 'CRIME/VIOLATION IN PROGRESS'))

# Group by 'ForceType' and count occurrences
force_type_counts = filtered_df.groupBy('ForceType').count().orderBy('count')

# Convert PySpark DataFrame to Pandas DataFrame for plotting
force_type_counts_pd = force_type_counts.toPandas()

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(force_type_counts_pd['ForceType'], force_type_counts_pd['count'], color='red')
plt.xlabel('Force Type')
plt.ylabel('Count')
plt.title('Count of CRIME/VIOLATION IN PROGRESS encounters by Force Type for BLACK race')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('gs://nyc-analytics/crime_violation_progress_by_force_type_black_race.png')  # Save plot to Cloud Storage
plt.close()

# Plot 4: Count of Suspicious Activity encounters by Force Type for HISPANIC race

# Filter the DataFrame for race type as 'HISPANIC' and basis of occurrence as 'SUSPICIOUS ACTIVITY'
filtered_df = use_of_force_combined.filter((col('SUBJECT RACE') == 'HISPANIC') & (col('BasisForEncounter') == 'SUSPICIOUS ACTIVITY'))

# Group by 'ForceType' and count occurrences
force_type_counts = filtered_df.groupBy('ForceType').count().orderBy('count')

# Convert PySpark DataFrame to Pandas DataFrame for plotting
force_type_counts_pd = force_type_counts.toPandas()

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(force_type_counts_pd['ForceType'], force_type_counts_pd['count'], color='skyblue')
plt.xlabel('Force Type')
plt.ylabel('Count')
plt.title('Count of Suspicious Activity encounters by Force Type for HISPANIC race')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('gs://nyc-analytics/suspicious_activity_by_force_type_hispanic_race.png')  # Save plot to Cloud Storage
plt.close()

# Stop SparkSession
spark.stop()
