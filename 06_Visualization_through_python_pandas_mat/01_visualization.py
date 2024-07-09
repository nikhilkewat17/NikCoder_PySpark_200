from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

# Initialize a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create a sample PySpark DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Convert the PySpark DataFrame to a Pandas DataFrame
pandas_df = df.toPandas()

# Plot a simple bar chart
pandas_df.plot(kind='bar', x='Name', y='Age', legend=False)
plt.xlabel('Name')
plt.ylabel('Age')
plt.title('Age of Individuals')
plt.show()