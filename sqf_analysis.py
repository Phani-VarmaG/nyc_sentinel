from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib as plt

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SQF Data Analysis") \
    .getOrCreate()

# Read data from Cloud SQL
jdbc_url = "jdbc:postgresql://104.155.165.91:5432/SQF"
properties = {
    "user": "nyc-processed",
    "password": "BigData2024",
    "driver": "org.postgresql.Driver"
}
sqf_data = spark.read.jdbc(url=jdbc_url, table="SQF_Data", properties=properties)

# Plot number of arrests by month
arrests_by_month = sqf_data.filter(sqf_data['SUSPECT_ARRESTED_FLAG'] == 'Y') \
    .groupBy('MONTH2').count().orderBy('MONTH2')

months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
counts = [0] * len(months)
for row in arrests_by_month.collect():
    month_index = months.index(row['MONTH2'])
    counts[month_index] = row['count']

plt.figure(figsize=(10, 6))
plt.plot(months, counts, marker='o', linestyle='-')
plt.title('Number of Arrests by Month')
plt.xlabel('Month')
plt.ylabel('Number of Arrests')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()

# Plot number of arrests by borough
arrests_by_borough = sqf_data.filter(sqf_data['SUSPECT_ARRESTED_FLAG'] == 'Y') \
    .groupBy('STOP_LOCATION_BORO_NAME').count().orderBy('count', ascending=False)

boroughs = [row['STOP_LOCATION_BORO_NAME'] for row in arrests_by_borough.collect()]
arrest_counts = [row['count'] for row in arrests_by_borough.collect()]

plt.figure(figsize=(10, 6))
plt.bar(boroughs, arrest_counts, color='blue')
plt.title('Number of Arrests by Borough')
plt.xlabel('Borough')
plt.ylabel('Number of Arrests')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.tight_layout()
plt.show()

# Plot total number of stops by month
incidents_by_month = sqf_data.groupBy('MONTH2').count().orderBy('MONTH2')

months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
counts = [0] * len(months)
for row in incidents_by_month.collect():
    month_index = months.index(row['MONTH2'])
    counts[month_index] = row['count']

plt.figure(figsize=(10, 6))
plt.plot(months, counts, marker='o', linestyle='-')
plt.title('Total Number of Stops by Month')
plt.xlabel('Month')
plt.ylabel('Total Number of Incidents')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()

# Stop SparkSession
spark.stop()
