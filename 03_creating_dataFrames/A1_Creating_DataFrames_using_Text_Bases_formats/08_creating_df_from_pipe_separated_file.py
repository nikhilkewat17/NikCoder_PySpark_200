from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("creating dataframe from pipe separated file").getOrCreate()

df = spark.read.option("header","true").option("delimiter","|").csv(r"data/users.pipe_separated.txt")

df.printSchema()

df.show()

spark.stop()

