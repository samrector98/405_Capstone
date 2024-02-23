import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# This file includes all functions and variables related to the database that will be accessed by the main program.

def load_data_from_file(file_name):
    input_file = open(file_name)
    data = []
    for line in input_file:
        entry = json.loads(line)
        data.append(entry)
    
    input_file.close()
    return data


# Scehma variables to use when initially gathering data from the provided files
branch_schema = StructType([
    StructField("BRANCH_CODE", IntegerType(), False),
    StructField("BRANCH_NAME", StringType(), True),
    StructField("BRANCH_STREET", StringType(), True),
    StructField("BRANCH_CITY", StringType(), True),
    StructField("BRANCH_STATE", StringType(), True),
    StructField("BRANCH_ZIP", StringType(), True),
    StructField("BRANCH_PHONE", StringType(), True),
    StructField("LAST_UPDATED", StringType(), True)
])
credit_schema = StructType([
    StructField("TRANSACTION_ID", IntegerType(), False),
    StructField("TRANSACTION_TYPE", StringType(), True),
    StructField("TRANSACTION_VALUE", FloatType(), True),
    StructField("DAY", StringType(), True),
    StructField("MONTH", StringType(), True),
    StructField("YEAR", StringType(), True),
    StructField("CUST_CC_NO", StringType(), True),
    StructField("CUST_SSN", IntegerType(), False),
    StructField("BRANCH_CODE", IntegerType(), False)
])
customer_schema = StructType([
    StructField("SSN", IntegerType(), False),
    StructField("FIRST_NAME", StringType(), True),
    StructField("MIDDLE_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("CREDIT_CARD_NO", StringType(), True),
    StructField("STREET_NAME", StringType(), True),
    StructField("APT_NO", StringType(), True),
    StructField("CUST_CITY", StringType(), True),
    StructField("CUST_STATE", StringType(), True),
    StructField("CUST_COUNTRY", StringType(), True),
    StructField("CUST_ZIP", StringType(), True),
    StructField("CUST_PHONE", StringType(), True),
    StructField("CUST_EMAIL", StringType(), True),
    StructField("LAST_UPDATED", StringType(), True)
])

# Multi-line variables to use as queries for the mySQL section in the main program
query_create_branch_table = """ CREATE TABLE IF NOT EXISTS CDW_SAPP_BRANCH (
    BRANCH_CODE INT PRIMARY KEY,
    BRANCH_NAME VARCHAR(255),
    BRANCH_STREET VARCHAR(255),
    BRANCH_CITY VARCHAR(255),
    BRANCH_STATE VARCHAR(255),
    BRANCH_ZIP VARCHAR(255),
    BRANCH_PHONE VARCHAR(255),
    LAST_UPDATED DATETIME
); """
query_create_credit_table = """
CREATE TABLE IF NOT EXISTS CDW_SAPP_CREDIT_CARD (
    TRANSACTION_ID INT PRIMARY KEY,
    TRANSACTION_TYPE VARCHAR(255),
    TRANSACTION_VALUE DECIMAL,
    TIMEID VARCHAR(255),
    CUST_CC_NO VARCHAR(255),
    CUST_SSN INT,
    BRANCH_CODE INT,
    
    FOREIGN KEY (BRANCH_CODE) REFERENCES CDW_SAPP_BRANCH(BRANCH_CODE),
    FOREIGN KEY (CUST_SSN) REFERENCES CDW_SAPP_CUSTOMER(SSN)
); """
query_create_customer_table = """
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
); """