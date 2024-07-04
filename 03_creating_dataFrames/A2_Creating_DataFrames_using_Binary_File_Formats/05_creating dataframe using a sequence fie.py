from pyspark import SparkContext
from pyspark.sql import SparkSession, Row

# Initialize spark context
sc = SparkContext(appName="ReadSequenceFile")

# Initialize spark session
spark = SparkSession(sc)

# read the sequence file into an RDD of key-value pairs
RDD = sc.sequenceFile(r"data/employee")

row_RDD = RDD.map(lambda kv : Row(employee_id = kv[0],employee_name = kv[1]))

# create data frame
df = spark.createDataFrame(row_RDD)

df.show()

sc.stop()