from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import IntegerType
from textblob import TextBlob
from pyspark.ml.feature import StringIndexer, Imputer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SQF Data ML Model") \
    .getOrCreate()

# Load preprocessed data from Cloud SQL table
jdbc_url = "jdbc:postgresql://104.155.165.91:5432/SQF"
properties = {
    "user": "nyc-processed",
    "password": "BigData2024",
    "driver": "org.postgresql.Driver"
}

# Read preprocessed data
preprocessed_data = spark.read.jdbc(url=jdbc_url, table="SQF_Data", properties=properties)

# Convert SUSPECT_REPORTED_AGE column from string to integer
preprocessed_data = preprocessed_data.withColumn("SUSPECT_REPORTED_AGE", col("SUSPECT_REPORTED_AGE").cast("int"))

# Convert "Y" to 1 and "N" to 0 in SUSPECT_ARRESTED_FLAG column
preprocessed_data = preprocessed_data.withColumn("SUSPECT_ARRESTED_FLAG", when(col("SUSPECT_ARRESTED_FLAG") == "Y", 1).otherwise(0))

# Convert "Y" to 1 and "N" to 0 in FRISKED_FLAG column
preprocessed_data = preprocessed_data.withColumn("FRISKED_FLAG", when(col("FRISKED_FLAG") == "Y", 1).otherwise(0))

# Function to perform sentiment analysis
def get_sentiment(text):
    if text:
        sentiment_score = TextBlob(text).sentiment.polarity * 5 + 5
        return int(sentiment_score)
    else:
        return 3  # Assign 5 to null values

# User Defined Function (UDF) for sentiment analysis
sentiment_udf = udf(get_sentiment, IntegerType())

# Apply sentiment analysis to the "DEMEANOR_OF_PERSON_STOPPED" column
preprocessed_data = preprocessed_data.withColumn("DEMEANOR_SENTIMENT", sentiment_udf("DEMEANOR_OF_PERSON_STOPPED"))

# Select relevant features and target variable
selected_features = ["SUSPECT_REPORTED_AGE", "SUSPECT_RACE_DESCRIPTION", "SUSPECT_SEX", 
                     "OBSERVED_DURATION_MINUTES", "STOP_DURATION_MINUTES", "DEMEANOR_SENTIMENT", 
                     "FRISKED_FLAG"]
target_variable = "SUSPECT_ARRESTED_FLAG"

# Drop rows with null values for numerical columns
preprocessed_data = preprocessed_data.select(selected_features + [target_variable]).dropna(subset=["SUSPECT_REPORTED_AGE", "OBSERVED_DURATION_MINUTES", "STOP_DURATION_MINUTES"])

# Impute null values with mean for numerical columns and most frequent value for categorical columns
imputer = Imputer(strategy="mean", inputCols=["SUSPECT_REPORTED_AGE", "OBSERVED_DURATION_MINUTES", "STOP_DURATION_MINUTES"], outputCols=["SUSPECT_REPORTED_AGE", "OBSERVED_DURATION_MINUTES", "STOP_DURATION_MINUTES"])
preprocessed_data = imputer.fit(preprocessed_data).transform(preprocessed_data)

# Handle categorical columns separately
categorical_cols = ["SUSPECT_RACE_DESCRIPTION", "SUSPECT_SEX"]
for col_name in categorical_cols:
    most_frequent_value = preprocessed_data.groupBy(col_name).count().orderBy("count", ascending=False).first()[col_name]
    preprocessed_data = preprocessed_data.fillna(most_frequent_value, subset=[col_name])

# Split the data into training and testing sets
(train_data, test_data) = preprocessed_data.randomSplit([0.7, 0.3], seed=42)

# Encode SUSPECT_RACE_DESCRIPTION and SUSPECT_SEX using StringIndexer
race_indexer = StringIndexer(inputCol="SUSPECT_RACE_DESCRIPTION", outputCol="SUSPECT_RACE_INDEX")
sex_indexer = StringIndexer(inputCol="SUSPECT_SEX", outputCol="SUSPECT_SEX_INDEX")

# Assemble features into a single vector
assembler = VectorAssembler(inputCols=["FRISKED_FLAG", "SUSPECT_REPORTED_AGE", "SUSPECT_RACE_INDEX", 
                                       "SUSPECT_SEX_INDEX", "OBSERVED_DURATION_MINUTES", 
                                       "STOP_DURATION_MINUTES", "DEMEANOR_SENTIMENT"],
                            outputCol="features")

# Define the Decision Tree model
dt = DecisionTreeClassifier(labelCol=target_variable, featuresCol="features")

# Create a pipeline
pipeline = Pipeline(stages=[race_indexer, sex_indexer, assembler, dt])

# Train the model
model = pipeline.fit(train_data)

# Make predictions on the test data
predictions = model.transform(test_data)

# Display predictions
predictions.select("SUSPECT_ARRESTED_FLAG", "prediction", "probability").show(5)

# Save the model to nyc-analytics bucket
model_path = "gs://nyc-analytics/sqf_decision_tree_model"
model.save(model_path)

# Stop SparkSession
spark.stop()
