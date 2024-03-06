from pyspark.sql import SparkSession
import mysql.connector
import requests
import login_info
import database_setup
import transaction_details
import customer_details

# Set up the spark session
spark = SparkSession.builder.appName("cdw_sapp").getOrCreate()

print("Establishing connection to server...")

# Establish a connection to our mySQL server.
connection = mysql.connector.connect(
    host = login_info.mysql_host,
    user = login_info.mysql_username,
    password = login_info.mysql_password
)
cursor = connection.cursor()

print("Connection established.")

print("Creating database...")
cursor.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone")
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

#Re-establish connection to our SQL server, this time under the creditcard_capstone database.
connection = mysql.connector.connect(
    host = login_info.mysql_host,
    user = login_info.mysql_username,
    password = login_info.mysql_password,
    database = "creditcard_capstone"
)
cursor = connection.cursor()

#Now, setup the tables using the mySQL queries stored in our data_loading file
print("Creating database tables...")
cursor.execute(database_setup.query_create_branch_table)
cursor.execute(database_setup.query_create_customer_table)
cursor.execute(database_setup.query_create_credit_table)
cursor.execute(database_setup.query_create_loan_table)
connection.commit()

# Now that we are done using the mysql connector, close the cursor and connection.
cursor.close()
connection.close()

# Get the data from the local files
print("Loading data from input files...")
branch_data = database_setup.load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/cdw_sapp_branch.json")
credit_data = database_setup.load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/cdw_sapp_credit.json")
customer_data = database_setup.load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/cdw_sapp_customer.json")

# Create DataFrames from the lists derived from the input files
branch_df = spark.createDataFrame(branch_data, schema = database_setup.branch_schema)
credit_df = spark.createDataFrame(credit_data, schema = database_setup.credit_schema)
customer_df = spark.createDataFrame(customer_data, schema = database_setup.customer_schema)

# The next step is to perform data transformation as outlined in the mapping document
print("Cleaning data...")
new_branch_df = database_setup.modify_branch_data(branch_df)
new_credit_df = database_setup.modify_credit_data(credit_df)
new_customer_df = database_setup.modify_customer_data(customer_df)

url = "jdbc:mysql://" + login_info.mysql_host + "/creditcard_capstone"

# Sending modified dataframes to the mySQL database
print("Sending clean data to the database...")
new_branch_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", "CDW_SAPP_BRANCH") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()

new_customer_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()

new_credit_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()

print("Database setup complete.")

# Now, get the data from the API provided in the instructions
print("Connecting to API...")
response = requests.get('https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')
loan_data = response.json()

# Print out the response code
print("API Endpoint Status Code: {}".format(response.status_code))

# Create the dataframe for the loan API information using the data list and our custom schema
print("Loading data from API...")
loan_df = spark.createDataFrame(loan_data, database_setup.loan_schema)

loan_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", "CDW_SAPP_LOAN_APPLICATION") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()

print("API data loaded successfully.")

# Lastly, the program will enter the main loop of user input and respective output.
menu = """\nWhat operation would you like to perform?

1. Display all transactions in a zip code for the specified month and year.
2. Display the number of transactions and their total value for the specified transaction type.
3. Display the number of transactions and their total value for all branches within the specified state.
4. Display all account details of the specified customer.
5. Modify the account details of the specified customer.
6. Display a report for the transactions on a credit card for the specified month and year.
7. Display all transactions made by a customer over the specified time frame.

Enter a number from the above options, or enter '0' to exit the program.
Your choice: """

menu_choice = input(menu).strip()

if menu_choice != '0':
    # Create the temporary views that any of the functions might use, since most of them will not change.
        new_branch_df.createOrReplaceTempView("branch_table")
        new_credit_df.createOrReplaceTempView("credit_table")
        new_customer_df.createOrReplaceTempView("customer_table")
        loan_df.createOrReplaceTempView("loan_table")

while menu_choice != '0':

    if menu_choice == '1':
        transaction_details.get_transactions_by_zip_code()
        
    elif menu_choice == '2':
        transaction_details.get_transactions_by_type()

    elif menu_choice == '3':
        transaction_details.get_transactions_by_state()

    elif menu_choice == '4':
        customer_details.get_customer_details()
        
    elif menu_choice == '5':
        new_customer_df = customer_details.update_customer_details(new_customer_df)

        # We must also replace the temporary view with the newest dataframe, since it is not synced with the server change
        new_customer_df.createOrReplaceTempView("customer_table")
    
    elif menu_choice == '6':
        customer_details.get_monthly_card_statement()
        
    elif menu_choice == '7':
        customer_details.get_transactions_by_customer()

    else:
        print("Error. The option you entered was not recognized. Please try again.")
    
    menu_choice = input(menu).strip()

print("Exiting program...")