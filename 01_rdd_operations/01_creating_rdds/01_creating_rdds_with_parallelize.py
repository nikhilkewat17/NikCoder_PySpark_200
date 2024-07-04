from pyspark import SparkContext

sc = SparkContext("local","Parallelize-Example")

list = [1,2,3,4,5,6,7,8,9,10]

RDD = sc.parallelize(list)

print(RDD.collect())