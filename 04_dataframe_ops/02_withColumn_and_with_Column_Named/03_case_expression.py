from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# create spark session
spark = SparkSession.builder.appName("Case Expression excercise").getOrCreate()

# create structure or schema
schema = StructType([
    StructField("eid",IntegerType(),True),
    StructField("ename",StringType(),True),
    StructField("dept",StringType(),True),
    StructField("salary",FloatType(),True),
    StructField("date_of_joining",DateType(),True)
])

# Create DataFrame
df = spark.read.format("csv").option("header","true").schema(schema).load(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\02_withColumn_and_with_Column_Named\data\employee.csv")

# create temporary view
df.createOrReplaceTempView("employee")
# Print Structure of DataFrame
df.printSchema()

# Showing all data
df.show()

# ------/---------------------/---------------------------/---------------------------/-----------------------/---------------/---------------/-----------

# Exercise 1 : Data Frame Case Expression salary range
# using expr
df.withColumn("Salary_category",expr('''
case
when salary > 100000 then "High_salary"
when salary > 50000 then "Medium_salary"
else "Low_salary"
end
'''))
df.show()

# using dataframe functions
df.withColumn("Salary_category",
              when(col("salary") > 100000 , "High_Salary").
              when(col("salary") > 60000 , "Medium_salary").
              otherwise("Low_salary")
              ).show(5)

# SQL
spark.sql(''' select * , 
case
  when salary > 100000 then "High_salary"
  when salary > 60000 then "Medium_salary"
  else "Low_salary"
end as salary_category
  from employee ''').show(1)

# ------/---------------------/---------------------------/---------------------------/-----------------------/---------------/---------------/-----------


# Exercise 2 : Experience Level
df.withColumn("Experience_Level" , expr('''case 
when year(date_of_joining) < 2010 then "Veteran"
when year(date_of_joining) <= 2015 then "Experienced"
else "Novice"
end
 ''')).show()

# using DateFrame function
df.withColumn("Experienced_Level",
              when(year("date_of_joining") < 2010 , "Veteran").
              when(year("date_of_joining") <= 2015 , "Experienced").
              otherwise("Novice")).show()
# SQL
spark.sql('''select * , 
case
 when year(date_of_joining) < 2010 then 'Veteran' 
 when year(date_of_joining) <= 2015 then 'Experienced'
 else 'Novice'
end as salary_category
 from employee''').show()

# ------/---------------------/---------------------------/---------------------------/-----------------------/---------------/---------------/-----------

# Excercise 3 : Department_alias
df.withColumn("Department_alias",expr('''
case
 when dept == 'HR' then "Human Resources"
 when dept == 'Tech' then "Technical"
 when dept == 'Finance' then "Accounting Sector"
 else "Marketing & Sales"
end 
 ''')).show()
# Data Frame function
df.withColumn("Department Form",
              when(col("dept")=='HR' , "Human_Resources").
              when(col("dept") == 'Tech' , "Technical").
              when(col("dept") == 'Finance',"Accounting").
              otherwise("Marketing & Sales")
              ).show()
# SQL
spark.sql('''select * ,
case 
    when dept == 'HR' then "Human Resources"
    when dept == 'Tech' then "Technical"
    when dept == 'Finance' then "Accounting"
    else "Marketing & Sales"
end as Department
from employee''').show()

# ------/---------------------/---------------------------/---------------------------/-----------------------/---------------/---------------/-----------

# Excercise 4 : Is high earner
avg_salary = df.selectExpr("avg(salary)").first()[0]
df.withColumn("is_high_earner",col("salary") > lit(avg_salary)).show()
# SQL
spark.sql("select * , salary > (select avg(salary) from employee) as is_high_earner from employee").show()

# ------/---------------------/---------------------------/---------------------------/-----------------------/---------------/---------------/-----------

# Excercise 5 : Adjust Salary
df.withColumn("Adjust_salary", expr('''case when dept = 'Tech' then salary * 1.1 else salary * 1.05 end ''')).show()

# DataFrame function
df.withColumn("Adjust_Salary",when(col("dept")=='Tech' , col("salary") * 1.1).otherwise(col("salary")*1.05)).show()

# SQL
spark.sql("select * , case when dept =='Tech' then salary * 1.1 else salary * 1.05 end as Adjust_salary from employee").show()

# ------/---------------------/---------------------------/---------------------------/-----------------------/---------------/---------------/-----------


# Excercise 6 : Tenure Years
df.withColumn("tenure_years", year(current_date()) - year("date_of_joining")).show()
df.withColumn("Tenure_years",expr("year(current_date()) - year(date_of_joining) ")).show() # using expr
spark.sql("""SELECT *, YEAR(current_date()) - YEAR(date_of_joining) as tenure_years FROM employees""").show() # SQL

# ------/---------------------/---------------------------/---------------------------/-----------------------/---------------/---------------/-----------


# Excercise 7 : Role Type
#df.withColumn("Role",when((col("dept") =='Tech') & (col("salary")>100000),"Sr.Manager").otherwise("General_staf")).show()
df.withColumn("Role",
              when((col("dept") =='Tech') & (col("salary")>100000),"Sr. Tech Manager").
              when((col("dept")=='HR') & (col("salary")>50000),"Sr.HR").
              otherwise("General_staf")
              ).show()
# SQL
spark.sql("""select * , 
case
    when dept == 'Tech' and salary > 100000 then "Sr.Technician"
    when dept == 'HR' and salary > 50000 then "Sr.HR"
    when dept == 'Finance' and salary > 60000 then "Sr.Accountant"
    else "General Staf"
end as Role
from employee""").show()

# Excercise 8 : Hire date session
df.withColumn("Hire_date_season",
              when(col(month("date_of_joining").isin(1,2,3)),"Winter")).show()

df.withColumn("season",
              when(month("date_of_joining").isin(1,2,3),"Winter").
              when(month("date_of_joining").isin(4,5,6),"Summer").
              when(month("date_of_joining").isin(7,8,9),"Mansoom").otherwise("Neutral")
              ).show(5)
# SQL
spark.sql(""" 
select * ,
case 
    when month(date_of_joining) in (1,2,3) then "Winter"
    when month(date_of_joining) in (4,5,6) then "Spring"
    when month(date_of_joining) in (7,8,9) then "Summer"
    else "Fall"
end as Hire_season from employee """).show()



# Excercise 9 : Normalization

min_salary , max_salary = df.selectExpr("min(salary)","max(salary)").first()
df1 = df.withColumn("Salary_normalization",(col("salary") - lit(min_salary)) / (lit(max_salary) - lit(min_salary)))
df1.show()

# SQL
spark.sql("""select * , 
( salary - (select min(salary) from employee)) / 
((select max(salary) from employee) - (select min(salary) from employee)) 
as salary_normalization 
from employee """).show()

# stop spark session
spark.stop()