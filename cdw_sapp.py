import mysql.connector
import requests
from pyspark.sql import SparkSession

import database_setup
import login_info

# Upon program start-up, print a message to show the program is running.

# Set up the spark session
spark_app = SparkSession.builder.appName("sparkdemo").getOrCreate()

# If the database does not exist...
# Establish a connection to our mySQL server.
connection = mysql.connector.connect(
    host = "localhost",
    user = login_info.mysql_username,
    password = login_info.mysql_password
)
cursor = connection.cursor()

# Create the database we will use for the rest of the project, and commit the change.
cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")
connection.commit()

# Close the cursor and connection in order to re-open it with the new database as a parameter.
cursor.close()
connection.close()

#Re-establish connection to our SQL server, this time under the creditcard_capstone database.
connection = mysql.connector.connect(
    host = "localhost",
    user = login_info.mysql_username,
    password = login_info.mysql_password,
    database = "creditcard_capstone"
)
cursor = connection.cursor()

#Now, setup the tables using the mySQL queries stored in our data_loading file
cursor.execute(database_setup.query_create_branch_table)
cursor.execute(database_setup.query_create_customer_table)
cursor.execute(database_setup.query_create_credit_table)
cursor.execute(database_setup.query_create_loan_table)
connection.commit()

# Now that we are done using the mysql connector, close the cursor and connection.
cursor.close()
connection.close()

# Get the data from the local files
branch_data = database_setup.load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/cdw_sapp_branch.json")
credit_data = database_setup.load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/cdw_sapp_credit.json")
customer_data = database_setup.load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/cdw_sapp_customer.json")

# Create DataFrames from the lists derived from the input files
branch_df = spark_app.createDataFrame(branch_data, schema = database_setup.branch_schema)
credit_df = spark_app.createDataFrame(credit_data, schema = database_setup.credit_schema)
customer_df = spark_app.createDataFrame(customer_data, schema = database_setup.customer_schema)

# Get the data from the API provided in the instructions
response = requests.get('https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')
loan_data = response.json()

# Print out the response code
print(response.status_code)

# Create the dataframe for the loan API information using the data list and our custom schema
loan_df = spark_app.createDataFrame(loan_data, database_setup.loan_schema)

# The next step is to perform data transformation as outlined in the mapping document
new_branch_df = database_setup.modify_branch_data(branch_df)
new_credit_df = database_setup.modify_credit_data(credit_df)
new_customer_df = database_setup.modify_customer_data(customer_df)

new_branch_df.printSchema()
new_branch_df.show()

new_credit_df.printSchema()
new_credit_df.show()

new_customer_df.printSchema()
new_customer_df.show()

loan_df.printSchema()
loan_df.show()

# Sending modified dataframes to the mySQL database

loan_df.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_LOAN_APPLICATION") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()

new_branch_df.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_BRANCH") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()

new_customer_df.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()

new_credit_df.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()

# If the database does exist...
# Create dataframes from each of the relevant tables in the database

# Lastly, the program will enter the main loop of user input and respective output.