from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("Employee Data Analysis").getOrCreate()

# Load the data into a DataFrame
df = spark.read.csv("data/employee.csv", header=True , inferSchema= True)

# Register the DataFrame as a temporary view to use SQL
df.createOrReplaceTempView("Employee")  # temprory Table name is employee

# Exercise 1: Select All Columns
df.show()
# Alternative using select with *
df.select("*").show()
# SQL equivalent
spark.sql("select * from Employee").show()

# Excercise 2 : Select specific columns
df.select("ename","dept").show()
#Alternative using selectExpr
df.selectExpr("ename","dept").show()
# SQL
spark.sql("select ename,dept from Employee").show()

# Excercise 3 : Select with column rename
df.select(df.salary.alias("monthly_salary")).show()
df.select(col("salary").alias("monthly_salary")).show()
# alternative using selectExpr
df.selectExpr("salary as monthly_salary").show()
# sql
spark.sql("select salary as monthly_salary from Employee").show()


# Excercise 4 : Select and perform an operations
df.select((df.salary / 12 ).alias("monthly_salary")).show()
df.select((col("salary") / 12).alias("monthly_salary")).show()
# alternative using selectExpr
df.selectExpr("salary / 12 as monthly_salary").show()
# SQL
spark.sql("select salary / 12 as monthly_salary from Employee").show()


# Execercise 5 : Filter by one condition
df.filter(df.dept == "Tech").show()
# Alternative using where
df.where(df.dept == "Tech").show()
# SQL
spark.sql("select * from employee where dept = 'Tech' ").show()

# Excercise 6 : Filter by Multiple conditions
df.filter((df.dept == "Finance") & (df.salary > 1000000)).show()
df.where((df.dept == "Finance") & (df.salary > 1000000)).show()
# SQL
spark.sql("select * from employee where dept = 'Finance' and salary > 1000000").show()spark


# Exercise 7: Date Filter
df.filter(df.date_of_joining > "2015-01-01").show()
# Alternative using where
df.where(df.date_of_joining > "2015-01-01").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE date_of_joining > '2015-01-01'").show()


# Exercise 8: Select and Filter Combination
df.select("eid", "ename").filter(df.dept == "HR").show()
# Alternative using where
df.select("eid", "ename").where(df.dept == "HR").show()
# SQL Equivalent
spark.sql("SELECT eid, ename FROM employee WHERE dept = 'HR'").show()


# Exercise 9: Multiple Conditions with Select
df.select("ename", "dept", "date_of_joining").filter((df.salary > 75000) & (df.dept == "Marketing")).show()
# Alternative using where with selectExpr
df.selectExpr("ename", "dept", "date_of_joining").where("salary > 75000 AND dept = 'Marketing'").show()
# SQL Equivalent
spark.sql("SELECT ename, dept, date_of_joining FROM employee WHERE salary > 75000 AND dept = 'Marketing'").show()


# Exercise 10: Complex Combination
df.select("ename", "salary").filter((df.date_of_joining < "2010-01-01") & (df.salary < 80000)).show()
# Alternative using where with selectExpr
df.selectExpr("ename", "salary").where("date_of_joining < '2010-01-01' AND salary < 80000").show()
# SQL Equivalent
spark.sql("SELECT ename, salary FROM employee WHERE date_of_joining < '2010-01-01' AND salary < 80000").show()

spark.stop()