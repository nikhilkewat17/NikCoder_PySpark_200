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

# Exercise 1 : Data Frame Case Expression
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
df.withColumn("Salary_category",when(col("salary") > 100000 , "High_Salary").when(col("salary") > 60000 , "Medium_salary").otherwise("Low_salary")).show(5)

# SQL
spark.sql(''' select * , 
case
  when salary > 100000 then "High_salary"
  when salary > 60000 then "Medium_salary"
  else "Low_salary"
end as salary_category
  from employee ''').show(1)


# stop spark session
spark.stop()