import mysql.connector
import pyspark
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
cursor.execute(database_setup.query_create_credit_table)
cursor.execute(database_setup.query_create_customer_table)
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

branch_df.printSchema()
branch_df.show()

credit_df.printSchema()
credit_df.show()

customer_df.printSchema()
customer_df.show()

# If the database does exist...
# Create dataframes from each of the relevant tables in the database

# Lastly, the program will enter the main loop of user input and respective output.

