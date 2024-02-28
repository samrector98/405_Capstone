import mysql.connector
import requests
from pyspark.sql import SparkSession

import database_setup
import login_info
import transaction_details

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

menu = """Please select from the following options:

1. Display all transactions in a specific zip code over a specified time frame.
2. Display all transactions for a specific transaction type.
3. Display all transactions from all branches located in a specific state.
4. Display the account details of a specific customer.
5. Modify the account details of a specific customer.
6. Generate a monthly report containing the transactions on a specific credit card.
7. Display all transactions made by a specific customer over a specified time frame.

Or, enter '0' to exit the program.

Your choice: """

user_input = input(menu).strip()

while user_input != '0':

    if user_input == '1':
        # Create or replace the temporary views needed by the function
        new_credit_df.createOrReplaceTempView("credit_table")
        new_customer_df.createOrReplaceTempView("customer_table")
        
        # Call the function
        results_1 = transaction_details.get_transactions_by_zip_code()
        
        # Display the results
        results_1.show()
        
    elif user_input == '2':
        # Create or replace the temporary view needed by the function
        new_credit_df.createOrReplaceTempView("credit_table")
        
        # Call the function
        results_2 = transaction_details.get_transactions_by_type()
        
        # Display the results
        results_2.show()
        print("The number of transactions found for the chosen transaction type was {}.".format(results_2.count()))
        # output the total value of the transactions

    elif user_input == '3':
        # Create or replace the temporary views needed by the function
        new_branch_df.createOrReplaceTempView("branch_table")
        new_credit_df.createOrReplaceTempView("credit_table")

        # Call the function
        results_3 = transaction_details.get_transactions_by_state()

        # Display the results
        results_3.show()
        print("The number of transactions found for the chosen transaction type was {}.".format(results_3.count()))
        # output the total value of the transactions

    elif user_input == '4':
        break
    elif user_input == '5':
        break
    elif user_input == '6':
        break
    elif user_input == '7':
        break
    else:
        print("Error. The option you entered was not recognized. Please try again.")
    
    user_input = input(menu).strip()

print("Exiting program...") 