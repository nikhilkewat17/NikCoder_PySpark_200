from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Register spark session
spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()
columns = ["Region", "Department", "Gender", "Salary", "Sales"]
data = [
    ("North", "Sales", "Male", 60000, 100),
    ("South", "HR", "Female", 75000, 200),
    ("East", "Marketing", "Female", 72000, 150),
    ("West", "Sales", "Male", 50000, 250),
    ("North", "HR", "Male", 82000, 300),
    ("South", "Marketing", "Female", 68000, 120),
    ("East", "HR", "Male", 72000, 110),
    ("West", "Marketing", "Female", 54000, 210),
    ("North", "Sales", "Female", 68000, 180),
    ("South", "HR", "Male", 75000, 190)
]

# Register data frame df and load data into that .
df = spark.createDataFrame(data , schema=columns)

# Register dataframe as temporary view use for SQL
df.createOrReplaceTempView("people")

# Register window
windo = Window.partitionBy("Department").orderBy("Salary")

# Excercise 1 : Row Number
df.withColumn("Row Number",row_number().over(windo)).show()
# SQL
spark.sql("select * , row_number() over(partition by Department order by Salary) as Row_number from people").show()

# Exercise 2 : Rank function
df.withColumn("Rank" , rank().over(windo)).show()
# SQL
spark.sql("select Department , rank() over(partition by Department order by Salary) as RANK from people").show()

# Exercise 3 : Dense_rank
df.withColumn("Dense_rank",dense_rank().over(windo)).show()
# SQL
spark.sql("select *  , dense_rank() over(partition by Department order by Salary) as Dense_salary from people").show()

# Exercise 4 : circumm SUM Salary
df.withColumn("cumlative_salary",sum("Salary").over(windo)).show()
# SQL
spark.sql("select * , sum(Salary) over(partition by Department order by Salary) as Cumlative_sum from people").show()

# Stop spark session .
spark.stop()
