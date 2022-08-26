import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan, when, count

# ----- 4.1 -----
response = requests.get("https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json")

# ----- 4.2 -----
print(response)

# ----- 4.3 -----
spark = SparkSession.builder.appName("LoanApp").getOrCreate()
rdd = spark.sparkContext.parallelize([response.json()])
loan_df = spark.read.json(rdd)
loan_df.show()

loan_df.write.format("jdbc") \
             .mode("append") \
             .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
             .option("dbtable", "CDW_SAPP_LOAN_APPLICATION") \
             .option("user", "root") \
             .option("password", "password") \
             .save()