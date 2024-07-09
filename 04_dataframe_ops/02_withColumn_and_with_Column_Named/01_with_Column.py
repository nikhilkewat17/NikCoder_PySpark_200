from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("withColumn").getOrCreate()

schema = StructType([
    StructField("eid",IntegerType(),True),
    StructField("ename",StringType(),True),
    StructField("dept",StringType(),True),
    StructField("salary",IntegerType(),True),
    StructField("date_of_joining",DateType(),True)
])

# create one dataframe
df = spark.read.format("csv").option("header","true").schema(schema).load(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\02_withColumn_and_with_Column_Named\data\employee.csv")

# print schema of dataframe
df.printSchema()

# Initialize temporary view of DF
df.createOrReplaceTempView("employee")

# Excercise 1 : Update department Names to uppercase
df.withColumn("Department_in_Upper",expr("upper(dept)")).show()
# SQL
spark.sql("select * , upper(dept) as Department_in_uppercase from employee").show()

# Excercise 2 : Add a new Column showing monthly salary
df.withColumn("monthly_salary",expr("salary / 12") ).show()
# SQL
spark.sql("select * , salary / 12 as Monthly_salary from employee").show()

# Excercise 3: Create boolean column chechking if salary is Above AVG
avg_salary = df.selectExpr("avg(salary)").first()[0]
df.withColumn("Above_avg_salary",df.salary > avg_salary).show()
# SQL
spark.sql("select * , salary > (select avg(salary) from employee) as Above_avg_salary from employee").show()

# Exercise 4: Add tenure column showing years since joining
df.withColumn("Tenure",expr("year(current_date()) - year(date_of_joining)")).show()
# SQL
spark.sql("select * , year(current_date()) - year(date_of_joining) as tenure_years_in_organisation from employee").show()

# Exercise 5: Create Column to categories salaries
df.withColumn("Salary_category",expr("case when salary > 100000 then 'High' when salary > 50000 then 'Medium' else 'low' end")).show()
# SQL
spark.sql("""select * , case
          when salary > 100000 then 'High'
          when salary > 50000 then 'Medium'
          else 'Low' end as salary_category
          from employee """).show()
# Excercise 6: Adjust salary for a 10% Raise across the board
df.withColumn("Adjusted_salary",df.salary * 1.1).show()
df.withColumn("adjusted salary",expr("salary * 1.1")).show()
# sql
spark.sql("select * , salary * 1.1 as adjusted_salary from employee").show()


# Exercise 7: Convert Date of Joining to 'YYYY-MM' Format
df.withColumn("joining_month_year", expr("date_format(date_of_joining, 'yyyy-MM')")).show()
# SQL Equivalent
spark.sql("SELECT *, date_format(date_of_joining, 'yyyy-MM') as joining_month_year FROM employee").show()

# Exercise 8: Create an Age Column Assuming All Employees are Born in 1985
df.withColumn("age", expr("year(current_date()) - 1985")).show()
# SQL Equivalent
spark.sql("SELECT *, YEAR(current_date()) - 1985 as age FROM employee").show()

# Exercise 9: Append a Suffix to Employee Names
df.withColumn("ename_suffix", expr("concat(ename, ' - Emp')")).show()
# SQL Equivalent
spark.sql("SELECT *, CONCAT(ename, ' - Emp') as ename_suffix FROM employee").show()

# Exercise 10: Normalize Salaries Between 0 and 1 Based on Min/Max Salary
min_salary, max_salary = df.selectExpr("min(salary)", "max(salary)").first()
df.withColumn("normalized_salary", (df.salary - min_salary) / (max_salary - min_salary)).show()
# SQL Equivalent
spark.sql("""
SELECT *, (salary - (SELECT MIN(salary) FROM employee)) / ((SELECT MAX(salary) FROM employee) - (SELECT MIN(salary) FROM employee)) as normalized_salary 
FROM employee
""").show()


spark.stop()
