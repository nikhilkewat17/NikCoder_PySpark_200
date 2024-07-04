from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create dataframe with nested JSON file").getOrCreate()


# use multiline option for nested json file.

df = spark.read.option("multiLine","true").json(r"data/nested_json.json")

df.printSchema()

df.show()

spark.stop()