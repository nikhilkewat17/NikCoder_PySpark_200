from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Creating Date Frame from json file").getOrCreate()


movies = spark.read.option("inferSchema","true").json("data/movies.json")

movies.printSchema()

movies.show()

spark.stop()