import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pyspark.sql.functions as f

# read/load data from database
loan_df = pd.read_sql_table("CDW_SAPP_LOAN_APPLICATION", "mysql://localhost:3306/creditcard_capstone")

# ----- 5.1 -----
married_male = loan_df.where((loan_df["Gender"] == "Male") & \
                            (loan_df["Application_Status"] == "Y") & \
                            (loan_df["Married"] == "Yes")) \
                      .groupby("Income") \
                      .size().reset_index(name="Count") \
                      .set_index("Income") \
                      .sort_values(by=["Count"], ascending=False)
married_female = loan_df.where((loan_df["Gender"] == "Female") & \
                              (loan_df["Application_Status"] == "Y") & \
                              (loan_df["Married"] == "Yes")) \
                        .groupby("Income") \
                        .size().reset_index(name="Count") \
                        .set_index("Income") \
                        .sort_values(by=["Count"], ascending=False)
print(married_male.head())
print(married_female.head())

x_axis = np.arange(len(married_male.index))
fig, ax = plt.subplots(figsize=(12, 8))
ax.bar(x_axis - 0.2, married_male["Count"].to_list(), 0.4, label="Married Male")
ax.bar(x_axis + 0.2, married_female["Count"].to_list(), 0.4, label="Married Female")

plt.xlabel("Income Range")
plt.xticks(x_axis, married_male.index.to_list())
plt.ylabel("Number of Application Approved")
plt.title("Total Approved Application for Married Men vs. Married Women", fontsize=18)
plt.legend()
# plt.show()

# ----- 5.2 -----
property_area_approved = loan_df.where(loan_df["Application_Status"] == "Y") \
                                .groupby("Property_Area") \
                                .size().reset_index(name="Count") \
                                .set_index("Property_Area")
print(property_area_approved.head())

x_axis = np.arange(len(property_area_approved.index))
fig, ax = plt.subplots(figsize=(12, 8))
ax.bar(x_axis, property_area_approved["Count"].to_list())

plt.xlabel("Property Area")
plt.xticks(x_axis, property_area_approved.index.to_list())
plt.ylabel("Number of Application Approved")
plt.title("Total Approved Application for Each Property Area", fontsize=18)
# plt.show()

# ----- Challenge -----
app_demographic_approved = loan_df.where(loan_df["Application_Status"] == "Y") \
                                  .groupby(by=["Credit_History", 
                                              "Dependents",
                                              "Education",
                                              "Gender",
                                              "Income",
                                              "Married",
                                              "Property_Area",
                                              "Self_Employed"]) \
                                  .size().reset_index(name="Count") \
                                  .sort_values(by=["Count"], ascending=False)
app_demographic_approved["Credit_History"] = app_demographic_approved["Credit_History"].astype(int)
print(app_demographic_approved.head())

x_axis = np.arange(len(app_demographic_approved.index))
labels = []
for row in app_demographic_approved.values:
    labels.append(f"{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}")
fig, ax = plt.subplots(figsize=(14, 10))
bar1 = ax.bar(x_axis, app_demographic_approved["Count"].to_list(), color=np.random.rand(len(x_axis), 3))
plt.xlabel("Application Demographic")
plt.xticks([])
plt.ylabel("Number of Application Approved")
plt.suptitle("Total Approved Application for Each Application Demographic", fontsize=18)
plt.title("Descending Order")
plt.legend(bar1, labels)
plt.show()