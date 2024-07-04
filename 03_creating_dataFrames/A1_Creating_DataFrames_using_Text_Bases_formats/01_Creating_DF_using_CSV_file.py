from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Creating Data Frame using CSV").getOrCreate()

df = spark.read.option("inferSchema","true").option("header","true").csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\03_creating_dataFrames\A1_Creating_DataFrames_using_Text_Bases_formats\data\employee.txt")

df.printSchema()

df.show()

spark.stop()