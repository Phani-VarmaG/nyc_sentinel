import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Shooting Incidents Analysis") \
    .getOrCreate()

# Connect to the Cloud SQL database
jdbc_url = "jdbc:postgresql://104.155.165.91:5432/SQF"
properties = {
    "user": "nyc-processed",
    "password": "BigData2024",
    "driver": "org.postgresql.Driver"
}

# Read data from the database
shooting_data = spark.read.jdbc(url=jdbc_url, table="Shooting_Incidents", properties=properties)

# Plot 1: Number of Shooting Incidents by Perpetrator Race

# Filter out null values in the PERP_RACE column
shooting_data_filtered = shooting_data.filter(shooting_data['PERP_RACE'].isNotNull())

# Group by perpetrator race and count the number of shooting incidents for each race
shooting_by_race = shooting_data_filtered.groupBy('PERP_RACE').count()

# Convert PySpark DataFrame to Pandas DataFrame for plotting
shooting_by_race_pd = shooting_by_race.toPandas()

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(shooting_by_race_pd['PERP_RACE'], shooting_by_race_pd['count'], color='orange')
plt.title('Number of Shooting Incidents by Perpetrator Race')
plt.xlabel('Perpetrator Race')
plt.ylabel('Number of Shooting Incidents')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.grid(axis='y')
plt.tight_layout()
plt.savefig('gs://nyc-analytics/shooting_incidents_by_perpetrator_race.png')  # Save plot to Cloud Storage
plt.close()

# Plot 2: Number of Shooting Incidents by Borough

# Group by borough and count the number of shooting incidents for each borough
shooting_by_borough = shooting_data.groupBy('BORO').count()

# Convert PySpark DataFrame to Pandas DataFrame for plotting
shooting_by_borough_pd = shooting_by_borough.toPandas()

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(shooting_by_borough_pd['BORO'], shooting_by_borough_pd['count'], color='red')
plt.title('Number of Shooting Incidents by Borough')
plt.xlabel('Borough')
plt.ylabel('Number of Shooting Incidents')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.grid(axis='y')
plt.tight_layout()
plt.savefig('gs://nyc-analytics/shooting_incidents_by_borough.png')  # Save plot to Cloud Storage
plt.close()

import folium
from folium.plugins import MarkerCluster

# Load PySpark DataFrame into a Pandas DataFrame
shooting_data_pd = shooting_data.toPandas()

# Drop rows with NaN values in Latitude or Longitude columns
shooting_data_pd = shooting_data_pd.dropna(subset=['Latitude', 'Longitude'])

# Create a map centered around New York City
nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=10)

# Create a MarkerCluster to group markers at the same location
marker_cluster = MarkerCluster().add_to(nyc_map)

# Add markers for each incident
for index, row in shooting_data_pd.iterrows():
    folium.Marker([row['Latitude'], row['Longitude']], popup=row['LOCATION_DESC']).add_to(marker_cluster)

# Save the map as an HTML file
nyc_map.save('gs://nyc-analytics/nyc_shooting_map.html')


# Stop SparkSession
spark.stop()
