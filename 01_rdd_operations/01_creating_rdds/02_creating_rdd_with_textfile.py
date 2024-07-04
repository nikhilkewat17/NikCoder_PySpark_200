from pyspark import SparkContext

sc = SparkContext("local" , "Create RDD with textFile")

RDD = sc.textFile(r"data/movies.csv")

print(RDD.collect())

sc.stop()