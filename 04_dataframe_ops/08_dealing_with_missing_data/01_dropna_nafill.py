from pyspark.sql import SparkSession , Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Drop_na_fill").getOrCreate()

data = [
    ("Alice", 25, None),
    ("Bob", None, 200),
    ("Charlie", 30, 300),
    ("David", 29, None),
    ("Ella", None, None),
    ("Frank", 26, 150),
    ("Grace", 28, None),
    (None , None , None),
    (None , 34 , 250)
]
df = spark.createDataFrame(data, ["Name", "Age", "Sales"])


df.createOrReplaceTempView("people")

# Exercises with solutions on handling missing data

# # Exercise 1: Drop any rows that contain missing data
# df.na.drop().show()
# spark.sql("SELECT * FROM people WHERE Age IS NOT NULL AND Sales IS NOT NULL").show()
# spark.sql("select * from people where Name is not null or Age is not null or Sales is not null").show()
#
# # Exercise 2: Drop rows where all columns are missing
# df.na.drop('all').show()
# spark.sql("select * from people where not(Name is null and Age is null and Sales is null)").show()
#
# # Exercise 3: Drop rows where any column is missing
# df.na.drop('any').show()
# spark.sql("select * from people where Name is not null and Age is not null and Sales is not null").show()
#
# # Exercise 4: Drop rows where missing in specific columns
# df.na.drop(subset = 'Name').show()
# #or
# df.na.drop('Name').show()
# spark.sql("select * from people where Name is not null").show()
#
# # Exercise 4: Drop rows where missing in specific columns
# df.na.drop(subset = 'Age').show()
# #or
# df.na.drop('Age').show()
# spark.sql("select * from people where Age is not null").show()

# Exercise 5: Fill all missing values with zeros
df.na.fill(0).show()
spark.sql("select * , coalesce(Name,'Unk') as Name , coalesce(Age,27) as Age , coalesce(Sales,150) as Sales from people").show()

# Exercise 6: Fill missing values with specific values for each column .
df.na.fill({"Age":27}).show()
spark.sql("select * , coalesce(Age,27) as Age from people").show()

# Exercise 8: Filter out rows with missing sales data .
df.filter(col("sales").isNotNull()).show()
spark.sql("SELECT * FROM people WHERE Sales IS NOT NULL").show()

# Exercise 7: Fill missing values using the mean of the column .
mean_age = df.select(avg("Age")).first()[0]
df.na.fill({"Age":mean_age}).show()
spark.sql(f"select Name , coalesce(Age,{mean_age}) from people").show()


# Exercise 9: Replace null values in 'Sales' with the average sales .
avg_sales = df.select(avg("Sales")).first()[0]
df.na.fill({"Sales": avg_sales}).show()
spark.sql(f"SELECT Name, Age, COALESCE(Sales, {avg_sales}) AS Sales FROM people").show()


# Exercise 13: Replace null values before performing an aggregation +++++
df.na.fill({"Sales": 0}).groupBy("Name").sum("Sales").show()
spark.sql("SELECT Name, SUM(COALESCE(Sales, 0)) AS Sales FROM people GROUP BY Name").show()

spark.stop()