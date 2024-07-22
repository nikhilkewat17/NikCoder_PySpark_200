from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create spark Session
spark = SparkSession.builder.appName("Joining Example").getOrCreate()

# create dataframe and load data into that.

# df_employee = spark.read.option("header","true").option("inferSchema","true").csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\05_Joins\data\Employees.csv")
# df_department =spark.read.option("header","true").option("inferSchema","true").csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\05_Joins\data\Departments.csv")

df_employee = spark.read.csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\05_Joins\data\Employees.csv",header = True , inferSchema = True)
df_department = spark.read.csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\05_Joins\data\Departments.csv",header = True , inferSchema = True)

# print Schema of dataframe
df_employee.printSchema()
df_department.printSchema()

# Register dataframe as temporary view use for SQL .
df_employee.createOrReplaceTempView("employee")
df_department.createOrReplaceTempView("departments")

# same column in both dataframe is DepartmentID .

# Exercise 1 : Inner Join
df_employee.join(df_department,df_employee.DepartmentID == df_department.DepartmentID,"inner").show()
# SQL
spark.sql("select * from employee as emp inner join departments as dept on emp.DepartmentID == dept.DepartmentID").show()

# Exercise 2 : Full Outer JOIN
df_employee.join(df_department , df_employee.DepartmentID == df_department.DepartmentID , "fullouter").show()
# SQL
spark.sql("select * from employee as emp full outer join departments as dept on emp.DepartmentID == dept.DepartmentID").show()


# Exercise 3 : Right join
df_employee.join(df_department , df_employee.DepartmentID == df_department.DepartmentID , "right").show()
# SQL
spark.sql("select * from employee as emp right join departments as dept on emp.DepartmentID == dept.DepartmentID").show()

# Exercise 4 : Left Anti Join
df_employee.join(df_department , df_employee.DepartmentID == df_department.DepartmentID , "leftanti").show()
# SQL
spark.sql("select * from employee as emp left anti join departments as dept on emp.DepartmentID == dept.DepartmentID").show()

# Exercise 5 : Left Semi Join
df_employee.join(df_department , df_employee.DepartmentID == df_department.DepartmentID , "leftsemi").show()
# SQL
spark.sql("select * from employee as emp left semi join departments as dept on emp.DepartmentID == dept.DepartmentID").show()

# Exercise 6 : Cross Join
df_employee.join(df_department , df_employee.DepartmentID == df_department.DepartmentID , "cross").show()
# SQL
spark.sql("select * from employee as emp cross join departments as dept on emp.DepartmentID == dept.DepartmentID").show()

# Stop spark session
spark.stop()