import requests
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv

# globals
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

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
             .option("user", user) \
             .option("password", password) \
             .save()