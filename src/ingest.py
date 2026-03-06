from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sales_pipeline").getOrCreate()

customers = spark.read.csv("/dbfs/data/customers.csv", header=True, inferSchema=True)
products = spark.read.csv("/dbfs/data/products.csv", header=True, inferSchema=True)
orders = spark.read.csv("/dbfs/data/orders.csv", header=True, inferSchema=True)

#customers.show(5)
#products.show(5)
#orders.show(5)
