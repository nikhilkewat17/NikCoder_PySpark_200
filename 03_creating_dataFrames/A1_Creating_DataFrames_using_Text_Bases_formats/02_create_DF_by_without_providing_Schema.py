from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , IntegerType , StringType , DoubleType  # import SQL Types for schema

spark = SparkSession.builder.appName("Create DataFrame without providing Schema").getOrCreate()

# Defining schema manually .
schema_nk = StructType([StructField("eid",IntegerType(),True),
                        StructField("ename",StringType(),True),
                        StructField("dept",StringType(),True),
                        StructField("salary",DoubleType(),True),
                        StructField("date_of_joining",StringType(),True)
                        ])

df = spark.read.option("header","true").schema(schema_nk).csv(r"data/employee.txt")

df.printSchema()

df.show()

spark.stop()