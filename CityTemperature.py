# Import SparkSession to create Spark application
from pyspark.sql import SparkSession

# Import aggregation functions
from pyspark.sql.functions import avg, sum, count, col

# Import schema types
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

if __name__ == "__main__":

    # Step 1: Create Spark session
    spark = SparkSession.builder.appName("CityTemperatureAnalysis").getOrCreate()

    # Step 2: Create the dataset
    data = [
        ("New York",10.0),
        ("New York",12.0),
        ("Los Angeles",20.0),
        ("Los Angeles",22.0),
        ("San Francisco",15.0),
        ("San Francisco",18.0)
    ]

    # Step 3: Define schema for DataFrame
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True)
    ])

    # Step 4: Create DataFrame
    df = spark.createDataFrame(data=data, schema=schema)

    print("Original Data")
    df.show()

    # Step 5: Group by city and calculate metrics
    result = df.groupBy("city").agg(
        avg("temperature").alias("avg_temperature"),
        sum("temperature").alias("total_temperature"),
        count("temperature").alias("num_measurements")
    )

    # Step 6: Keep only cities where total temperature > 30
    filtered = result.filter(col("total_temperature") > 30)

    # Step 7: Sort result by city name in ascending order
    final_result = filtered.orderBy("city")

    print("Final Result")
    final_result.show(truncate=False)
