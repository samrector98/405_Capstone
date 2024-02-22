import mysql.connector
import login_info

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

# Close the cursor and connection, will re-open with the new database as a parameter.
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

# Create the tables in mySQL that will be used to store our data in the database
cursor.execute("""
CREATE TABLE IF NOT EXISTS CDW_SAPP_BRANCH (
    BRANCH_CODE INT PRIMARY KEY,
    BRANCH_NAME VARCHAR(255),
    BRANCH_STREET VARCHAR(255),
    BRANCH_CITY VARCHAR(255),
    BRANCH_STATE VARCHAR(255),
    BRANCH_ZIP VARCHAR(255),
    BRANCH_PHONE VARCHAR(255),
    LAST_UPDATED DATETIME
); """)

cursor.execute("""
CREATE TABLE IF NOT EXISTS CDW_SAPP_CUSTOMER (
    SSN INT PRIMARY KEY,
    FIRST_NAME VARCHAR(255),
    MIDDLE_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    CREDIT_CARD_NO VARCHAR(255),
    FULL_STREET_ADDRESS VARCHAR(255),
    CUST_CITY VARCHAR(255),
    CUST_STATE VARCHAR(255),
    CUST_COUNTRY VARCHAR(255),
    CUST_ZIP VARCHAR(255),
    CUST_PHONE VARCHAR(255),
    CUST_EMAIL VARCHAR(255),
    LAST_UPDATED DATETIME
); """)

cursor.execute("""
CREATE TABLE IF NOT EXISTS CDW_SAPP_CREDIT_CARD (
    TRANSACTION_ID INT PRIMARY KEY,
    TRANSACTION_DATE DATE,
    CUST_CC_NO VARCHAR(255),
    CUST_SSN INT,
    BRANCH_CODE INT,
    TRANSACTION_TYPE VARCHAR(255),
    TRANSACTION_VALUE DECIMAL,
    
    FOREIGN KEY (BRANCH_CODE) REFERENCES CDW_SAPP_BRANCH(BRANCH_CODE),
    FOREIGN KEY (CUST_SSN) REFERENCES CDW_SAPP_CUSTOMER(SSN)
); """)

connection.commit()

# Close the cursor and connection, will re-open with the new database as a parameter.
cursor.close()
connection.close()