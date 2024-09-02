from pyspark.sql import SparkSession

# Initialize Spark Session using the PostgreSQL jar file
spark = SparkSession.builder \
    .appName("CSV to RDS") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.3.1.jar") \
    .getOrCreate()

# Define the JDBC URL and connection properties - Using the RDS  instance endpoint
jdbc_url = "jdbc:postgresql://datastream.cpgmsksiy1n5.us-east-2.rds.amazonaws.com:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "postgresql1234",
    "driver": "org.postgresql.Driver"
}

# Load the CSV file into a DataFrame
df = spark.read.csv("books_data.csv", header=True, inferSchema=True)

# Write the DataFrame to the PostgreSQL table
df.write.jdbc(url=jdbc_url, table="book", mode="append", properties=connection_properties)

# Stop the Spark session
spark.stop()
