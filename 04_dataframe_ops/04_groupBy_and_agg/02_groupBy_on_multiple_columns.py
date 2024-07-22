from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create spark .
spark = SparkSession.builder.appName("Group By on Multiple Column").getOrCreate()

# Creating dataframe .
df = spark.read.csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\04_groupBy_and_agg\data\region_data_emp.csv",header=True,inferSchema=True)

# print schema of dataframe .
df.printSchema()

 # Register the dataframe as temporary view to use SQL .
df.createOrReplaceTempView("sales")

# Excercise 1 : Exercise 1: Average Salary by Region and Department .
df.groupBy("Region","Department").agg(avg("Salary").alias("Avg_Salary")).show()
# SQL
spark.sql("select Region , Department , avg(Salary) as Average_salary from sales group by Region , Department").show()

# Exercise 2: Total Sales by Region and Gender .
df.groupBy("Region","Gender").agg(sum("Sales").alias("Total_sale")).show()
# # SQL
spark.sql("select Region , Gender , sum(Sales) as Total_Sales from sales group by Region,Gender").show()

# Exercise 3: Maximum and Minimum Salary by Department and Gender .
df.groupBy("Department","Gender").agg(max("Salary").alias("Max_salary"),min("Salary").alias("Min_Salary")).show()
# SQL
spark.sql(" select Department , Gender , max(Salary) as Max_salary , min(Salary) as Min_salary from sales group by Department , Gender ").show()

# Exercise 4: Count of Employees by Region, Department, and Gender .
df.groupBy("Region","Department","Gender").agg(count("*").alias("Number_Employee")).show()
# SQL
spark.sql("select Region , Department , Gender , count(*) as Number_of_Employee from sales group by Region , Department , Gender").show()

# Exercise 5: Average Sales by Region and Department .
df.groupBy("Region","Department").agg(avg("Salary").alias("Avg_salary")).show()
# # SQL
spark.sql("select Region , Department , avg(Salary) as AVG_Salary from sales group by Region , Department").show()

# Exercise 6: Sum of Salaries by Gender and Department .
df.groupBy("Department","Gender").agg(sum("Salary").alias("Sum_Salary")).show()
# SQL
spark.sql("select Department , Gender , sum(Salary) as Sum_salary from sales group by Department , Gender").show()

# Exercise 8: Count Distinct Departments in Each Region .
df.groupBy("Region").agg(countDistinct("Department").alias("Distinct_Dept_Region")).show()
# SQL
spark.sql("select Region , count(distinct Department) as Distinct_department from sales group by Region").show()

# Exercise 9: Minimum Sales by Region and Gender .
df.groupBy("Region","Gender").agg(min("Sales").alias("Minimum_sale")).show()
# SQL
spark.sql("select Region , Gender  , min(Sales) as Minimum_salas from sales group by Region , Gender ").show()

# Exercise 10: Total Number of Employees and Average Salary by Department and Gender.
df.groupBy("Department","Gender").agg(count("*").alias("Number_of_Employee"),avg("Salary").alias("Avg_salary")).show()
# SQL
spark.sql("select Department , Gender , count(*) as Number_Emp , avg(Salary) as AVG_Salary from sales group by Department , Gender ").show()


df.show()
# Stop spark
spark.stop()