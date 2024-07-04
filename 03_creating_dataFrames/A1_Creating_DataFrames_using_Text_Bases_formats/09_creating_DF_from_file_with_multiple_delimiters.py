from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("creating dataframe from mixed delimited file").getOrCreate()

df = spark.read.option("header","true").option("delimiter",",").csv(r"data/mixed_delimiters_data.txt")

df.printSchema()

df.show()

spark.stop()
