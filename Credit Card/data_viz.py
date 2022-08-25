import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.functions import col

# read/load data from database
credit_df = pd.read_sql_table("CDW_SAPP_CREDIT_CARD", "mysql://localhost:3306/creditcard_capstone")
customer_df = pd.read_sql_table("CDW_SAPP_CUSTOMER", "mysql://localhost:3306/creditcard_capstone")

# ----- 3.1 -----
transaction_type_count_df = credit_df.groupby("TRANSACTION_TYPE") \
                                     .size().reset_index(name="Count") \
                                     .set_index("TRANSACTION_TYPE") \
                                     .sort_values(by=["Count"])
print(transaction_type_count_df.head())

fig = transaction_type_count_df.plot(kind="barh", 
                                    figsize=(12, 8), 
                                    legend=None)
plt.suptitle("Total Occurances of Each Transaction Type", fontsize=18)
plt.title("Within Year 2018. Ascending Order by Transaction Count")
plt.ylabel("Transaction Type")
plt.yticks(rotation=0)
plt.xlabel("Number of Occurances")
plt.bar_label(fig.containers[0])
# plt.show()

# ----- 3.2 -----
customer_count_per_state_df = customer_df.groupby("CUST_STATE") \
                                         .size().reset_index(name="Count") \
                                         .set_index("CUST_STATE") \
                                         .sort_values(by=["Count"])
print(customer_count_per_state_df.head())

fig = customer_count_per_state_df.plot(kind="barh",
                                      figsize=(12, 8),
                                      legend=None)
plt.suptitle("Total Customers Per State", fontsize=18)
plt.title("Within Year 2018. Ascending Order by Customer Count")
plt.ylabel("State")
plt.yticks(rotation=0)
plt.xlabel("Number of Customers")
plt.bar_label(fig.containers[0])
# plt.show()

# ----- 3.3 -----
total_transaction_per_customer_df = credit_df.groupby("CUST_SSN") \
                                             .sum() \
                                             .drop(columns=["BRANCH_CODE", "TRANSACTION_ID"]) \
                                             .sort_values(by=["TRANSACTION_VALUE"]) \
                                             .tail(20)
print(total_transaction_per_customer_df.head())

fig = total_transaction_per_customer_df.plot(kind="barh",
                                            figsize=(12, 8),
                                            legend=None)
plt.suptitle("Total Transaction Amount for Each Customer", fontsize=18)
plt.title("Top 20 Customers with the Highest Transaction Amount (out of 952).\nWithin Year 2018. Ascending Order by Transaction Value")
plt.ylabel("Customer SSN")
plt.yticks(rotation=0)
plt.xlabel("Total Transaction Amount")
plt.bar_label(fig.containers[0])
# plt.show()

# ----- 3.4 -----
credit_df["MONTH"] = credit_df["TIMEID"].str.slice(4, 6)
total_transaction_per_month_df = credit_df.groupby("MONTH") \
                                          .sum() \
                                          .drop(columns=["CUST_SSN", "BRANCH_CODE", "TRANSACTION_ID"]) \
                                          .sort_values(by=["TRANSACTION_VALUE"])
print(total_transaction_per_month_df.head())
top_three = total_transaction_per_month_df.nlargest(n=3, columns=["TRANSACTION_VALUE"])
print(top_three)

fig = top_three.plot(kind="bar",
                    figsize=(12, 8),
                    legend=None)
plt.suptitle("Top 3 Month with the Largest Transaction Amount", fontsize=18)
plt.title("Within Year 2018. Ascending Order by Transaction Value")
plt.xlabel("Month")
plt.xticks(rotation=0)
plt.ylabel("Total Transaction Amount")
plt.bar_label(fig.containers[0])
# plt.show()

# ----- 3.5 -----
total_transaction_per_branch_healthcare_df = credit_df.where(credit_df["TRANSACTION_TYPE"] == "Healthcare") \
                                                      .groupby("BRANCH_CODE") \
                                                      .sum() \
                                                      .drop(columns=["CUST_SSN", "TRANSACTION_ID"]) \
                                                      .sort_values(by=["TRANSACTION_VALUE"])
total_transaction_per_branch_healthcare_df.index = total_transaction_per_branch_healthcare_df.index.astype(int)
print(total_transaction_per_branch_healthcare_df.head())

fig = total_transaction_per_branch_healthcare_df.plot(kind="bar",
                                                     figsize=(20, 9),
                                                     legend=None)
plt.suptitle("Total Healthcare Transaction Amount for Each Branch", fontsize=18)
plt.title("Within Year 2018. Ascending Order by Transaction Value")
plt.xlabel("Branch Code")
plt.xticks(rotation=90)
plt.ylabel("Total Transaction Amount")
plt.show()