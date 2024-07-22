from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create spark
spark = SparkSession.builder.appName("GroupByAndAggregation").getOrCreate()

# create dataframe
df = spark.read.csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\04_groupBy_and_agg\data\employee.csv" , header =True ,inferSchema = True)

# Print schema of dataframe
df.printSchema()

# register dataframe as temporary View
df.createOrReplaceTempView("employee")

# Use case 1 : Sort by column in ascending order
df.sort("salary").show()
# SQL
spark.sql("select * from employee order by salary")

# use case 2 : Sort by one column in descending order
df.sort(df.salary.desc()).show()
# SQL
spark.sql("select * from employee order by salary desc").show()

# Use case 3 : OrderBy with ascending and descending order
df.orderBy(col("dept").asc(),col("salary").desc()).show()
# SQL
spark.sql("select * from employee order by dept asc ,salary desc").show()

# Use case 4 : sort by multiple column
df.orderBy("dept","salary").show()
# SQL
spark.sql("select * from employee order by dept , salary").show()

# use case 5 : Using asc() & desc() in sort
df.sort(col("dept").asc(),col("salary").desc()).show()
# SQL
spark.sql("select * from employee order by dept asc , salary desc").show()










# stop spark
spark.stop()