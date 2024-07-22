from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("DistinctAndDropApp").getOrCreate()

# sample data
data = [
    ("Alice", "Data Science", 1000),
    ("Bob", "Engineering", 1500),
    ("Alice", "Data Science", 1000),
    ("David", "HR", 750),
    ("Bob", "Engineering", 1500),
    ("Ella", "Marketing", 1200),
    ("Frank", "Marketing", 1200),
    ("Alice", "Data Science", 1000),
    ("Gina", "HR", 750),
    ("Bob", "Engineering", 2000)
]

# define columns
column = ["Name", "Department", "Salary"]

# creating dataframe .
df = spark.createDataFrame(data,schema=column)

# Register dataframe as SQL table temporary .
df.createOrReplaceTempView("employee")

# Exercises with solutions on handling distinct and drop duplicates

# Exercise 1 : Remove all duplicates .
df.distinct().show()
spark.sql("select  distinct * from employee").show()

# Excercise 2 : Remove duplicates based on specific columns(Name and Department)
df.dropDuplicates(["Name","Department"]).show()
spark.sql("select distinct Name , Department , first(Salary) over(partition by Name , Department order by Salary ) as Salary from employee").show()


# Exercise 3: Remove duplicates and keep the row with the highest salary
df.withColumn("row_number",row_number().over(Window.partitionBy("Name","Department").orderBy(col("Salary").desc() ))).filter("row_number = 1").drop("row_number").show()
spark.sql("select Name , Department, max(Salary) as Salary from employee group by Name , Department ").show()

# Excercise 4 : Show distinct departments only .
df.select("Department").distinct().show()
spark.sql("select distinct Department from employee").show()

# Excercise 5 : Count of distinct Names .
print(df.select("Name").distinct().count())
spark.sql("select count(distinct Name) from employee").show()

# Excercise 6 : Count of distinct Department and Salary.
print(df.select("Department","Salary").distinct().count())
spark.sql("select count(distinct Department , Salary) from employee").show()

# Exercise 9: Show distinct salaries greater than 1000
df.select("Salary").distinct().filter("Salary > 1000").show()
spark.sql("SELECT DISTINCT Salary FROM employees WHERE Salary > 1000").show()

# Exercise 10: Get all distinct combinations of Name and Department
df.select("Name", "Department").distinct().show()
spark.sql("SELECT DISTINCT Name, Department FROM employees").show()
