# CityTemperatureAnalysis
This project builds a data processing pipeline using Apache Spark to analyze temperature measurements from multiple cities. The pipeline aggregates temperature data and generates useful metrics for each city.

Features

Load temperature data into a Spark DataFrame

Group data by city

Calculate key metrics:

Average temperature

Total temperature

Number of measurements

Filter cities where total temperature > 30

Sort the final output by city name

Technologies Used

Apache Spark

PySpark

Python

Output Metrics

The pipeline produces the following columns:

city

avg_temperature

total_temperature

num_measurements

The final dataset is sorted in ascending order by city.
