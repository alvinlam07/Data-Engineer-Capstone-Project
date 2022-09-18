import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, substring, lpad, concat, initcap, lower, concat_ws
from pyspark.sql.types import IntegerType, TimestampType, StringType
from dotenv import load_dotenv

load_dotenv()

# globals
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

# create spark session
spark = SparkSession.builder.appName("CreditCard").getOrCreate()

# ----- 1.1 -----

# load the json files and name the table
branch_df = spark.read.json("Credit Card/cdw_sapp_branch.json")
credit_df = spark.read.json("Credit Card/cdw_sapp_credit.json")
customer_df = spark.read.json("Credit Card/cdw_sapp_custmer.json")

# modify dataframes
branch_df = branch_df \
            .withColumn("BRANCH_CODE",
                        branch_df["BRANCH_CODE"].cast(IntegerType())) \
            .withColumn("BRANCH_ZIP",
                        branch_df["BRANCH_ZIP"].cast(IntegerType())) \
            .withColumn("LAST_UPDATED",
                        branch_df["LAST_UPDATED"].cast(TimestampType())) \
            .withColumn("BRANCH_PHONE",
                        format_string("(%s)%s-%s", substring("BRANCH_PHONE", 1, 3), substring("BRANCH_PHONE", 4, 3), substring("BRANCH_PHONE", 7, 4)))
branch_df.na.fill(value=00000, subset=["BRANCH_ZIP"])
branch_df = branch_df.select("BRANCH_CODE", \
                            "BRANCH_NAME", \
                            "BRANCH_STREET", \
                            "BRANCH_CITY", \
                            "BRANCH_STATE", \
                            "BRANCH_ZIP", \
                            "BRANCH_PHONE", \
                            "LAST_UPDATED")

credit_df = credit_df \
            .withColumn("CUST_CC_NO",
                        credit_df["CREDIT_CARD_NO"]) \
            .withColumn("CUST_SSN",
                        credit_df["CUST_SSN"].cast(IntegerType())) \
            .withColumn("DAY",
                        credit_df["DAY"].cast(StringType())) \
            .withColumn("MONTH",
                        credit_df["MONTH"].cast(StringType())) \
            .withColumn("YEAR",
                        credit_df["YEAR"].cast(StringType())) \
            .withColumn("BRANCH_CODE",
                        credit_df["BRANCH_CODE"].cast(IntegerType())) \
            .withColumn("TRANSACTION_ID",
                        credit_df["TRANSACTION_ID"].cast(IntegerType()))
credit_df = credit_df \
            .withColumn("DAY",
                        lpad(credit_df["DAY"], 2, "0")) \
            .withColumn("MONTH",
                        lpad(credit_df["MONTH"], 2, "0"))
credit_df = credit_df.select("CUST_CC_NO", \
                            concat(credit_df["YEAR"], credit_df["MONTH"], credit_df["DAY"]).alias("TIMEID"), \
                            "CUST_SSN", \
                            "BRANCH_CODE", \
                            "TRANSACTION_TYPE", \
                            "TRANSACTION_VALUE", \
                            "TRANSACTION_ID")

customer_df = customer_df \
              .withColumn("SSN",
                          customer_df["SSN"].cast(IntegerType())) \
              .withColumn("FIRST_NAME",
                          initcap(customer_df["FIRST_NAME"])) \
              .withColumn("MIDDLE_NAME",
                          lower(customer_df["MIDDLE_NAME"])) \
              .withColumn("LAST_NAME",
                          initcap(customer_df["LAST_NAME"])) \
              .withColumn("CUST_ZIP",
                          customer_df["CUST_ZIP"].cast(IntegerType())) \
              .withColumn("CUST_PHONE",
                          customer_df["CUST_PHONE"].cast(StringType())) \
              .withColumn("CUST_PHONE",
                          format_string("(000)%s-%s", substring("CUST_PHONE", 1, 3), substring("CUST_PHONE", 4, 7))) \
              .withColumn("LAST_UPDATED",
                          customer_df["LAST_UPDATED"].cast(TimestampType()))
customer_df = customer_df.select("SSN", \
                                "FIRST_NAME", \
                                "MIDDLE_NAME", \
                                "LAST_NAME", \
                                "CREDIT_CARD_NO", \
                                concat_ws(",", customer_df["APT_NO"], customer_df["STREET_NAME"]).alias("FULL_STREET_ADDRESS"), \
                                "CUST_CITY", \
                                "CUST_STATE", \
                                "CUST_COUNTRY", \
                                "CUST_ZIP", \
                                "CUST_PHONE", \
                                "CUST_EMAIL", \
                                "LAST_UPDATED")
# print(branch_df.printSchema())
# branch_df.show()
# print(credit_df.printSchema())
# credit_df.show()
# print(customer_df.printSchema())
# customer_df.show()

# ----- 1.2 -----

branch_df.write.format("jdbc") \
                .mode("append") \
                .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                .option("dbtable", "CDW_SAPP_BRANCH") \
                .option("user", user) \
                .option("password", password) \
                .save()

credit_df.write.format("jdbc") \
                .mode("append") \
                .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
                .option("user", user) \
                .option("password", password) \
                .save()

customer_df.write.format("jdbc") \
                .mode("append") \
                .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                .option("dbtable", "CDW_SAPP_CUSTOMER") \
                .option("user", user) \
                .option("password", password) \
                .save()