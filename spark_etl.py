from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Step 1: Create Spark session
spark = SparkSession.builder.appName("SalesETL").getOrCreate()

# Step 2: Load the CSV file
df = spark.read.option("header", "true").option("inferSchema", "true").csv("sales_data.csv")

# Step 3: Add a Total column (Quantity * Price)
df = df.withColumn("Total", col("Quantity") * col("Price"))

# Step 4: Group by Region and Product, and sum the Total
result = df.groupBy("Region", "Product").agg(_sum("Total").alias("TotalSales"))

# Step 5: Show the results
result.show()