from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, expr, col

# Initialize a Spark session
spark = SparkSession.builder.appName("PivotUnpivotDemo").getOrCreate()

# Sample data
data = [
    ("North", "Apple", 10),
    ("North", "Ap ple", 15),
    ("North", "Banana", 20),
    ("South", "Apple", 15),
    ("South", "Banana", 25),
    ("East", "Apple", 5),
    ("East", "Banana", 30),
    ("West", "Apple", 20),
    ("West", "Banana", 10)
]

# Columns: Region, Fruit, Sales
df = spark.createDataFrame(data, ["Region", "Fruit", "Sales"])

# Register DataFrame as a SQL table
df.createOrReplaceTempView("sales")

# Pivot Exercises
# Exercise 1: Pivot the data to show total sales for each fruit by region
pivot_df = df.groupBy("Region").pivot("Fruit").sum("Sales")
pivot_df.show()
# SQL Equivalent (not directly supported in Spark SQL, conceptual only)
spark.sql("SELECT Region, SUM(CASE WHEN Fruit = 'Apple' THEN Sales ELSE 0 END) AS Apple, SUM(CASE WHEN Fruit = 'Banana' THEN Sales ELSE 0 END) AS Banana FROM sales GROUP BY Region").show()

# Exercise 2: Pivot with multiple aggregate functions
pivot_df2 = df.groupBy("Region").pivot("Fruit").agg(sum(col("Sales")), sum(expr("Sales * 1.1")).alias("SP"))
pivot_df2.show()
# SQL Equivalent is similar, adding multiple aggregates per case

# Unpivot Exercises
# Exercise 3: Unpivot the DataFrame back to its original form
unpivot_expr = "stack(2, 'Apple', Apple, 'Banana', Banana) as (Fruit, Sales)"
unpivot_df = pivot_df.select("Region", expr(unpivot_expr))
unpivot_df.show()

spark.stop()