import mysql.connector
import re
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

# globals
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

# create spark session
spark = SparkSession.builder.appName("CreditCard").getOrCreate()

# create database connection
db = mysql.connector.connect(
    host="localhost",
    user=user,
    password=password,
    database="creditcard_capstone"
)

# load tables into dataframes
branch_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                        .option("dbtable", "CDW_SAPP_BRANCH") \
                        .option("user", user) \
                        .option("password", password) \
                        .load()
credit_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                        .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
                        .option("user", user) \
                        .option("password", password) \
                        .load()
customer_df = spark.read.format("jdbc") \
                        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                        .option("dbtable", "CDW_SAPP_CUSTOMER") \
                        .option("user", user) \
                        .option("password", password) \
                        .load()
# branch_df.show()
# credit_df.show()
# customer_df.show()
branch_df.createOrReplaceTempView("CDW_SAPP_BRANCH")
credit_df.createOrReplaceTempView("CDW_SAPP_CREDIT_CARD")
customer_df.createOrReplaceTempView("CDW_SAPP_CUSTOMER")

def transaction1():
    # check for valid zip code
    while True:
        zipcode = input("Enter Zip Code: ").strip()
        if len(zipcode) == 0 or not zipcode.isnumeric():
            print("Zip Code was empty or not a number. Try again...")
        else:
            break
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
    result = spark.sql(query)
    result.show(credit_df.count())
    print(f"{result.count()} row(s) fetched")

def transaction2():
    credit_df.select("TRANSACTION_TYPE").distinct().show()
    transaction_type = input("Select Transaction Type: ").strip()

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
    state = input("Enter State: ").strip()

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
    # check for valid ssn
    while True:
        ssn = input("Enter SSN: ").strip()
        if len(ssn) == 0 or not ssn.isnumeric():
            print("SSN was empty or not a number. Try again...")
        else:
            break
    query = \
    f"""
    SELECT *
    FROM CDW_SAPP_CUSTOMER 
    WHERE SSN = {ssn}
    """
    result = spark.sql(query)
    result.show()
    print(f"{result.count()} row(s) fetched")

def customer5():
    # check for valid ssn
    while True:
        ssn = input("Enter SSN: ").strip()
        if len(ssn) == 0 or not ssn.isnumeric():
            print("SSN was empty or not a number. Try again...")
        else:
            # check if ssn exist
            query = \
            f"""
            SELECT *
            FROM CDW_SAPP_CUSTOMER 
            WHERE SSN = {ssn}
            """
            result = spark.sql(query)
            if result.count() == 0:
                print("SSN does not exist. Try again.")
                continue
            break
    columns = customer_df.schema.names
    prompt = \
    f"""
    Which information do you want to modify?
    1. {columns[1]}
    2. {columns[2]}
    3. {columns[3]}
    4. {columns[4]}
    5. {columns[5]}
    6. {columns[6]}
    7. {columns[7]}
    8. {columns[8]}
    9. {columns[9]}
    10. {columns[10]}
    11. {columns[11]}
    d. Done modifying
    Input:
    """
    # check for valid user input
    while True:
        user_input = input(prompt).strip()
        if user_input in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"]:
            modify_account_detail(ssn, user_input)
        elif user_input == "d":
            break
        else:
            continue

def modify_account_detail(ssn, user_input):
    cursor = db.cursor()
    if user_input == "1":
        # check for empty input
        while True:
            new_first_name = input("Enter New First Name: ").strip().title()
            if len(new_first_name) == 0:
                print("First Name was empty. Try again.")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET FIRST_NAME = '{new_first_name}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "2":
        # check for empty string
        while True:
            new_middle_name = input("Enter New Middle Name: ").strip().lower()
            if len(new_middle_name) == 0:
                print("Middle Name was empty. Try again.")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET MIDDLE_NAME = '{new_middle_name}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "3":
        # check for empty string
        while True:
            new_last_name = input("Enter New Last Name: ").strip().title()
            if len(new_last_name) == 0:
                print("Last Name was empty. Try again.")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET LAST_NAME = '{new_last_name}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "4":
        # check for valid credit card no (string number only)
        while True:
            new_credit_card_no = input("Enter New Credit Card No: ").strip()
            if not new_credit_card_no.isnumeric():
                print("Inputted Credit Card No is invalid. Try again")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET CREDIT_CARD_NO = '{new_credit_card_no}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "5":
        # check for valid apt no (string number only)
        while True:
            new_apt_no = input("Enter New Apt No: ").strip()
            if not new_apt_no.isnumeric():
                print("Inputted Apt No is invalid. Try again")
            else:
                break
        # check for empty string
        while True:
            new_street_name = input("Enter New Street Name: ").strip()
            if len(new_street_name) == 0:
                print("Street Name was empty. Try again.")
            else:
                break
        new_full_address = f"{new_apt_no},{new_street_name}"
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET FULL_STREET_ADDRESS = '{new_full_address}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "6":
        # check for empty string
        while True:
            new_city = input("Enter New City: ").strip()
            if len(new_city) == 0:
                print("City was empty. Try again.")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET CUST_CITY = '{new_city}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "7":
        # check for empty or valid string
        while True:
            new_state = input("Enter New State: ").strip().upper()
            if len(new_state) == 0 or len(new_state) > 2:
                print("State was empty or invalid. Try again.")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET CUST_STATE = '{new_state}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "8":
        # check for empty string
        while True:
            new_country = input("Enter New Country: ").strip()
            if len(new_country) == 0:
                print("Country was empty. Try again.")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET CUST_COUNTRY = '{new_country}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "9":
        # check for empty and valid string
        while True:
            new_zip = input("Enter New Zip Code: ").strip()
            if len(new_zip) == 0 or not new_zip.isnumeric():
                print("Zip Code was empty or invalid. Try again.")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET CUST_ZIP = {new_zip}, LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "10":
        # check for empty and valid string
        while True:
            new_phone = input("Enter New Phone Number: ").strip()
            if len(new_phone) == 0 or not len(new_phone) == 10 or not new_phone.isnumeric():
                print("Phone Number was empty or invalid. Try again.")
            else:
                break
        new_phone = f"({new_phone[:3]}){new_phone[3:6]}-{new_phone[6:]}"
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET CUST_PHONE = '{new_phone}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    elif user_input == "11":
        # check for empty and valid string
        while True:
            new_email = input("Enter New Email: ").strip()
            email_regex = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")
            if len(new_email) == 0 or not email_regex.fullmatch(new_email):
                print("Email was empty or invalid. Try again.")
            else:
                break
        query = \
        f"""
        UPDATE CDW_SAPP_CUSTOMER
        SET CUST_EMAIL = '{new_email}', LAST_UPDATED = CURRENT_TIMESTAMP()
        WHERE SSN = {ssn}
        """
        cursor.execute(query)
        db.commit()
    print("Changes has been applied")
    print(cursor.rowcount, "row(s) affected")

def customer6():
    # check for valid ssn
    while True:
        credit_card_no = input("Enter Credit Card No: ").strip()
        if len(credit_card_no) == 0 or not credit_card_no.isnumeric():
            print("Credit Card No was empty or not a number. Try again...")
        else:
            break
    month = input("Enter Month: ").strip()
    year = input("Enter Year: ").strip()
    # add leading "0" if user inputs 1 digit number
    if len(month) == 1:
        month = "0" + month
    
    query = \
    f"""
    SELECT *
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CUST_CC_NO = {credit_card_no} and
        SUBSTRING(TIMEID, 1, 4) = '{year}' and 
        SUBSTRING(TIMEID, 5, 2) = '{month}'
    ORDER BY SUBSTRING(TIMEID, 7, 2);
    """
    result = spark.sql(query)
    result.show()
    total_cost = result.groupBy().sum("TRANSACTION_VALUE").collect()[0][0]
    print(f"Total spent that month: ${total_cost}\n")
    print(f"{result.count()} row(s) fetched")
    
def customer7():
    while True:
        ssn = input("Enter SSN: ").strip()
        if len(ssn) == 0 or not ssn.isnumeric():
            print("SSN was empty or not a number. Try again...")
        else:
            # check if ssn exist
            query = \
            f"""
            SELECT *
            FROM CDW_SAPP_CUSTOMER 
            WHERE SSN = {ssn}
            """
            result = spark.sql(query)
            if result.count() == 0:
                print("SSN does not exist. Try again.")
                continue
            break
    print("----- First Date -----")
    day = input("Enter Day: ").strip()
    month = input("Enter Month: ").strip()
    year = input("Enter Year: ").strip()
    # add leading "0" if user inputs 1 digit number for month and day
    if len(day) == 1:
        day = "0" + day
    if len(month) == 1:
        month = "0" + month
    print("----- Second Date -----")
    day2 = input("Enter Day: ").strip()
    month2 = input("Enter Month: ").strip()
    year2 = input("Enter Year: ").strip()
    # add leading "0" if user inputs 1 digit number for month and day
    if len(day2) == 1:
        day2 = "0" + day2
    if len(month2) == 1:
        month2 = "0" + month2
    
    query = \
    f"""
    SELECT *
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CUST_SSN = {ssn} and 
        TIMEID BETWEEN '{year + month + day}' and '{year2 + month2 + day2}'
    ORDER BY SUBSTRING(TIMEID, 1, 4), SUBSTRING(TIMEID, 5, 2), SUBSTRING(TIMEID, 7, 2)
    """
    result = spark.sql(query)
    result.show()
    print(f"{result.count()} row(s) fetched")

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
