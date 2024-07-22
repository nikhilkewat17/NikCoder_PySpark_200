from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("JoinsExample").getOrCreate()

# Creating two dataframe with two different file which has one same column to join it .
df_breakfast_order = spark.read.csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\05_Joins\data\breakfast_orders.txt",header=True)
df_token_detail = spark.read.csv(r"file:///C:\Users\Hp\PycharmProjects\nikcoder_pyspark_200\04_dataframe_ops\05_Joins\data\token_details.txt",header=True)

# Creating temporary view of dataframe use like SQL
df_breakfast_order.createOrReplaceTempView("breakfast_order")
df_token_detail.createOrReplaceTempView("token_detail")


# Excercise 1 : Inner JOIN
df_breakfast_order.join(df_token_detail,df_breakfast_order.token_color == df_token_detail.color,"inner").show()
# SQl
spark.sql("select * from breakfast_order as B inner join token_detail as T on B.token_color == T.color ").show()

# Excercise 2 : Left JOIN
df_breakfast_order.join(df_token_detail,df_breakfast_order.token_color == df_token_detail.color , "left").show()
# SQL
spark.sql("select * from breakfast_order as B left join token_detail as T on B.token_color == t.color").show()

# Excercise 3 : Right Join
df_breakfast_order.join(df_token_detail,df_breakfast_order.token_color == df_token_detail.color,"right").show()
# SQL
spark.sql("select * from breakfast_order as B right join token_detail as T on B.token_color == T.color").show()

# Exercise 4 : Full Outer Join
df_breakfast_order.join(df_token_detail , df_breakfast_order.token_color == df_token_detail.color,"fullouter").show()
# SQL
spark.sql("select * from breakfast_order as B full outer join token_detail as T on B.token_color == T.color").show()


# Excercise 5 : Left Semi Join
df_breakfast_order.join(df_token_detail,df_breakfast_order.token_color == df_token_detail.color,"leftsemi").show()
# SQL
spark.sql("select * from breakfast_order as B left semi join token_detail as T on B.token_color == T.color").show()


# Exercise 6 : Left Anti Join
df_breakfast_order.join(df_token_detail,df_breakfast_order.token_color == df_token_detail.color,"leftanti").show()
# SQL
spark.sql("select * from breakfast_order as B left anti join token_detail as T on B.token_color == T.color").show()

# Exercise 7 : Cross Join
df_breakfast_order.join(df_token_detail,df_breakfast_order.token_color == df_token_detail.color , "cross").show()
# SQL
spark.sql("select * from breakfast_order as B cross join token_detail as T on B.token_color == T.color").show()

# stop spark Session
spark.stop()