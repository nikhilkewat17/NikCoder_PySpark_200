from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row

# Initialize a Spark Session
spark = SparkSession.builder.appName("DataFrameOperationsDemo").getOrCreate()

# Sample data for two data Frame .

data1 = [Row(id=1, name="Alice", age=25),
         Row(id=2, name="Bob", age=30),
         Row(id=3, name="Charlie", age=35),
         Row(id=4, name="David", age=40),
         Row(id=5, name="Ella", age=45)]

data2 = [Row(id=3, name="Charlie", age=35),
         Row(id=4, name="David", age=40),
         Row(id=5, name="Ella", age=45),
         Row(id=6, name="Frank", age=50),
         Row(id=7, name="Grace", age=55)]

# creating data frames .
df1 = spark.createDataFrame(data1)
df2 = spark.createDataFrame(data2)

# Register dataframe as temporary view for SQL
df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

# # Exercise 1: Union of df1 and df2
# df1.union(df2).show()
# # SQL
# spark.sql("select * from table1 union all select * from table2").show()


# Exercise 2: Union Distinct of df1 and df2 .
#df1.union(df2).distinct().show()
# SQL
#spark.sql("select distinct * from table1 union select distinct * from table2").show()

# Exercise 3: Intersect of df1 and df2 .


# Show dataframes
# df1.show()
# df2.show()
# Stop Spark session
spark.stop()

# A. Basic Set Operations
#   1. Union of df1 and df2: How do you perform a union of two DataFrames to combine all rows from both, including duplicates?
#   2. Union Distinct of df1 and df2: How can you perform a union of two DataFrames that eliminates duplicate rows?

# B. Intersection and Difference
#   3. Intersect of df1 and df2: What method would you use to find rows that are common to both DataFrames?
#   4. Subtract df1 from df2: How do you determine rows in df2 that are not present in df1?
#   5. Subtract df2 from df1: Conversely, how do you find rows in df1 that are not in df2?

# C. Combining Multiple Operations
#   6. Union of Intersect and Subtract: How can you combine results of intersect and subtract operations between two DataFrames?
#   7. Symmetric Difference (Union - Intersect): What is the method to find rows in either df1 or df2 but not in both?

# D. Handling Duplicates
#   8. Duplicate Records After Union: After performing a union, how can you identify and display duplicate records?

# E. Additional Metadata in Unions
#   9. Add a column after Union to Identify Source DataFrame: How can you modify DataFrames before union to include a column indicating the source DataFrame?

# F. Complex Join Operations
#   10. Left Semi Join followed by a Union with Right Anti Join: How can you combine results of a left semi join and a right anti join between two DataFrames?

# Each question explores different aspects of manipulating DataFrames using both PySpark DataFrame operations and SQL queries, demonstrating how to achieve complex data transformations and analyses within a Spark environment.



