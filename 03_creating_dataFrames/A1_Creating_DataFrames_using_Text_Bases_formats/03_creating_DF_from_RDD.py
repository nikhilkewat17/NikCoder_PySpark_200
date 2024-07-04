from pyspark.sql import SparkSession



spark = SparkSession.builder.appName("Creating DataFrame with RDD").getOrCreate()




sc = spark.sparkContext

RDD = sc.textFile("data/employee.txt")






spark.stop()