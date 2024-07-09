from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame step up select Filter").getOrCreate()

df = spark.read.option("header","true").option("inferSchema","true").csv(r"data/employee.csv")

df.printSchema()

# Register the dataframe as temporary view
df.createOrReplaceTempView("employee")

# Excercise 11 : Select Employees with salary over $1000000
df.select("*").filter("salary > 1000000").show()
# sql
spark.sql("select * from employee where salary > 1000000").show()

# Excercise 12 : Select Employee Names and departement where department is not 'Tech'
df.select("ename","dept").filter("dept != 'Tech'").show()
#sql
spark.sql("select ename , dept from employee where dept != 'Tech'").show()

# Excercise 13: Use SelectExpr to calculate the monthly salary for each employee
df.selectExpr("salary / 12 as monthly_salary").show()
# sql
spark.sql("select ename , salary / 12 as monthly_salary from employee").show()

# Excercise 14: Filter Employee who joined Before 2015 and select their name and joining date
df.select("ename","date_of_joining").filter("date_of_joining < '2015-01-01'").show()
# sql
spark.sql("select ename , date_of_joining from employee where date_of_joining = '2015-01-01'").show()

# Exercise 15: select Employee Name with 'a' in Thier Name
df.select("ename").filter("ename like '%a%'").show()
# sql
spark.sql("select ename from employee where ename like '%a%'")

# Excercise 16 : Use selectExpr to show employees Name and boolean column if salary is above avarage
df.selectExpr("ename","salary > (select avg(salary) from employee) as above_avg_salary").show()
# sql
spark.sql("select ename , salary > (select avg(salary) from employee) as above_avg_salary from employee").show()
spark.sql("select ename , salary > (select avg(salary) from employee) as above_avg_salary from employee").show()

# Excercise 17 : Filter employee from 'Finance' deprtment and select employee name
df.select("*").filter("dept == 'Finance'").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE dept = 'Finance'").show()

# Ex 18 : use selectExpr to list employee and year of their joining
df.selectExpr("ename","year(date_of_joining) as joining_year").show()
# SQL Equivalent
spark.sql("SELECT ename, YEAR(date_of_joining) as joining_year FROM employee").show()

# Exercise 19: Filter for Employees Earning More Than $60,000 and in the 'HR' Department
df.filter("salary > 60000 AND dept = 'HR'").select("*").show()
# SQL Equivalent
spark.sql("SELECT * FROM employee WHERE salary > 60000 AND dept = 'HR'").show()

# Exercise 20: Select Employee Names and Use selectExpr to Show if They Joined in the Last Decade
df.selectExpr("ename", "(year(current_date()) - year(date_of_joining) <= 10) as joined_last_decade").show()
# SQL Equivalent
spark.sql("SELECT ename, (YEAR(current_date()) - YEAR(date_of_joining) <= 10) as joined_last_decade FROM employee").show()

spark.stop()