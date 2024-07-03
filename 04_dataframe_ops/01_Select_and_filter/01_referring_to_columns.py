from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ColumnReferenceDemo").getOrCreate()

# Create data frame and load data into that .
df = spark.read.option("header","true").option("inferSchema","true").csv("data/employee.csv")
df.printSchema()

# 1. Using column Names directly as strings
df.select("ename","dept").show()
df.filter("salary > 80000").show()

# 2. Using col() function
df.select(col("ename"),col("dept")).show()
df.filter(col("salary") > 80000 ).show()

# 3. Using dataFrame attribute style access .
df.select(df.ename , df.dept).show()
df.filter(df.salary > 80000).show()

# 4. Using selectExpr for SQL - Like Expressions .
df.selectExpr("ename as employee_name" , "dept as department_name").show()
df.selectExpr("avg(salary) as average_salary").show()

# 5. using SQL directly by registering a temp view.
df.createOrReplaceTempView("employees")
spark.sql("select * from employees where salary > 80000").show()
spark.sql("select ename as Employee_Name from employees").show()


spark.stop()