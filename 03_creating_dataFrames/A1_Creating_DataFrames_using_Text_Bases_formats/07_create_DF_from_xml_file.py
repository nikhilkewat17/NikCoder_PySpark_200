from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create dataframe with XML file").config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.14.0").getOrCreate()


df = spark.read.format("xml").option("rowTag","person").load(r"data/persons.xml")

df.printSchema()

df.show()

spark.stop()