import pyspark
from pyspark.sql import SparkSession

def transaction1():
    zipcode = input("Enter Zip Code: ").strip()
    month = input("Enter Month: ").strip()
    year = input("Enter Year: ").strip()

    # add leading "0" if user inputs 1 digit number
    if len(month) == 1:
        month = "0" + month

    query = \
    f"""
    SELECT cc.*
    FROM CDW_SAPP_CREDIT_CARD as cc, CDW_SAPP_CUSTOMER as cust
    WHERE cc.CUST_SSN = cust.SSN and 
        (cust.CUST_ZIP = {zipcode} and 
        SUBSTRING(cc.TIMEID, 1, 4) = '{year}' and 
        SUBSTRING(cc.TIMEID, 5, 2) = '{month}')
    ORDER BY SUBSTRING(cc.TIMEID, 7, 2) DESC;
    """
    # check for empty zipcode and return if is it
    try:
        result = spark.sql(query)
    except pyspark.sql.utils.ParseException:
        print("Zip Code was empty. Try again...")
        return
    result.show(credit_df.count())
    print(f"{result.count()} row(s) fetched")

def transaction2():
    credit_df.select("TRANSACTION_TYPE").distinct().show()
    transaction_type = input("Select Transaction Type: ")

    query = \
    f"""
    SELECT 
        TRANSACTION_TYPE, 
        COUNT(*), 
        ROUND(SUM(TRANSACTION_VALUE), 2)
    FROM CDW_SAPP_CREDIT_CARD AS cc
    WHERE TRANSACTION_TYPE = '{transaction_type}'
    GROUP BY TRANSACTION_TYPE;
    """
    result = spark.sql(query)
    result = result.withColumnRenamed(result.schema.names[1], "Number of Transactions") \
                   .withColumnRenamed(result.schema.names[2], "Total Cost")
    result.show()
    print(f"{result.count()} row(s) fetched")

def transaction3():
    state = input("Enter State: ")

    query = \
    f"""
    SELECT 
        b.BRANCH_CODE, 
        b.BRANCH_STATE, 
        COUNT(*), 
        ROUND(SUM(cc.TRANSACTION_VALUE), 2)
    FROM CDW_SAPP_CREDIT_CARD as cc, CDW_SAPP_BRANCH as b
    WHERE cc.BRANCH_CODE = b.BRANCH_CODE and
        (b.BRANCH_STATE = '{state}')
    GROUP BY b.BRANCH_CODE, b.BRANCH_STATE
    ORDER BY b.BRANCH_CODE;
    """
    result = spark.sql(query)
    result = result.withColumnRenamed(result.schema.names[2], "Number of Transactions") \
                   .withColumnRenamed(result.schema.names[3], "Total Cost")
    result.show()
    print(f"{result.count()} row(s) fetched")

def customer4():
    print(4)

def customer5():
    print(5)

def customer6():
    print(6)

def customer7():
    print(7)

# create spark session
spark = SparkSession.builder.appName("CreditCard").getOrCreate()

# load tables into dataframes
branch_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                        .option("dbtable", "CDW_SAPP_BRANCH") \
                        .option("user", "root") \
                        .option("password", "password") \
                        .load()
credit_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                        .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
                        .option("user", "root") \
                        .option("password", "password") \
                        .load()
customer_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                        .option("dbtable", "CDW_SAPP_CUSTOMER") \
                        .option("user", "root") \
                        .option("password", "password") \
                        .load()
# branch_df.show()
# credit_df.show()
# customer_df.show()
branch_df.createOrReplaceTempView("CDW_SAPP_BRANCH")
credit_df.createOrReplaceTempView("CDW_SAPP_CREDIT_CARD")
customer_df.createOrReplaceTempView("CDW_SAPP_CUSTOMER")

# spark.sql("SELECT * from CDW_SAPP_BRANCH").show(branch_df.count())

# ----- 2.1 -----
prompt = \
"""
Transaction Details:
1. Display the transactions made by customers living in a given zip code for a given month and year (Order by day in descending order)
2. Display the number and total values of transactions for a given type
3. Display the number and total values of transactions for branches in a given state
Customer Details:
4. Check the existing account details of a customer
5. Modify the existing account details of a customer
6. Generate a monthly bill for a credit card number for a given month and year
7. Display the transactions made by a customer between two dates. Order by year, month, and day in descending order
q. Quit the program
Input: 
"""
while(True):
    user_input = input(prompt).strip()
    if user_input == "1":
        transaction1()
    elif user_input == "2":
        transaction2()
    elif user_input == "3":
        transaction3()
    elif user_input == "4":
        customer4()
    elif user_input == "5":
        customer5()
    elif user_input == "6":
        customer6()
    elif user_input == "7":
        customer7()
    elif user_input == "q":
        quit()
