from pyspark.sql.functions import col, lit, concat, lower, initcap, format_string, substring
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DecimalType
import json

# This file includes all functions and variables related to the database that will be accessed by the main program.

def load_data_from_file(file_name):
    input_file = open(file_name)
    data = []
    for line in input_file:
        entry = json.loads(line)
        data.append(entry)
    
    input_file.close()
    return data

def modify_branch_data(df):

    # Convert the format of the phone number in branch to (XXX)XXX-XXXX, still as VARCHAR
    new_df = df.withColumn("BRANCH_PHONE", concat(lit("("), substring(col("BRANCH_PHONE"), 1, 3), lit(")"), substring(col("BRANCH_PHONE"), 4, 3), lit("-"), substring(col("BRANCH_PHONE"), 7, 4)))

    # If a branch entry has a null zip code, set it to a default value of '99999', still as VARCHAR
    new_df = new_df.na.fill({'BRANCH_ZIP': '99999'})

    # EXTRA: Some zip codes are incomplete due to leading zeroes, fix that...
    new_df = new_df.withColumn("BRANCH_ZIP", col("BRANCH_ZIP").cast("integer"))
    new_df = new_df.withColumn("BRANCH_ZIP", format_string("%05d", "BRANCH_ZIP"))
    new_df = new_df.withColumn("BRANCH_ZIP", col("BRANCH_ZIP").cast("string"))

    return new_df

def modify_credit_data(df):
    
    # Format the year, day, and month variables to have the proper number of digits each BEFORE combinging them into the new column
    new_df = df.withColumn("YEAR", col("YEAR").cast('integer'))
    new_df = new_df.withColumn("MONTH", col("MONTH").cast('integer'))
    new_df = new_df.withColumn("DAY", col("DAY").cast('integer'))
    
    new_df = new_df.withColumn("YEAR", format_string("%04d", "YEAR"))
    new_df = new_df.withColumn("MONTH", format_string("%02d", "MONTH"))
    new_df = new_df.withColumn("DAY", format_string("%02d", "DAY"))

    # Convert the day, month, year columns in credit_df into a single TIMEID column to represent the date
    new_df = new_df.withColumn("TIMEID", concat(col("YEAR"), col("MONTH"), col("DAY")))

    # Drop the columns that were combined into our new column, as we will not need duplicate info in our new version of the dataset
    new_df = new_df.drop("YEAR", "MONTH", "DAY")

    new_df = new_df.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")

    return new_df

def modify_customer_data(df):

    # Convert the customer's first name to Title Case
    new_df = df.withColumn("FIRST_NAME", initcap("FIRST_NAME"))
    
    # Convert the customer's middle name to lower case
    new_df = new_df.withColumn("MIDDLE_NAME", lower("MIDDLE_NAME"))

    # Convert the customer's last name to Title Case
    new_df = new_df.withColumn("LAST_NAME", initcap("LAST_NAME"))

    # Convert the format of the phone number in customer to (XXX)XXX-XXXX, still as VARCHAR
    # Since all the customer phone numbers only consisted of seven digits, I have temporarily assigned an area code of '000'. Customers would need to re-confirm their full phone number when able.
    new_df = new_df.withColumn("CUST_PHONE", concat(lit("(000)"), substring(col("CUST_PHONE"), 1, 3), lit("-"), substring(col("CUST_PHONE"), 4, 4)))

    # Concatenate the customer's street name and apt. number into the FULL_STREET_ADDRESS column, seperated by a comma
    new_df = new_df.withColumn("FULL_STREET_ADDRESS", concat(col("APT_NO"), lit(" "), col("STREET_NAME")))

    # Drop the columns that were combined into our new column, as we will not need duplicate info in our new version of the dataset
    new_df = new_df.drop("STREET_NAME", "APT_NO")

    # EXTRA: Implementing this code from branch, just in case the same issue arises with the customer zip codes
    new_df = new_df.withColumn("CUST_ZIP", col("CUST_ZIP").cast("integer"))
    new_df = new_df.withColumn("CUST_ZIP", format_string("%05d", "CUST_ZIP"))
    new_df = new_df.withColumn("CUST_ZIP", col("CUST_ZIP").cast("string"))

    return new_df

# Scehma variables to use when initially creating dataframes from the provided files
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
    StructField("CREDIT_CARD_NO", StringType(), True),
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

# Schema for the data acquired from the API
loan_schema = StructType([
    StructField("Application_ID", StringType(), False),
    StructField("Gender", StringType(), True),
    StructField("Married", StringType(), True),
    StructField("Dependents", StringType(), True),
    StructField("Education", StringType(), True),
    StructField("Self_Employed", StringType(), True),
    StructField("Credit_History", IntegerType(), True),
    StructField("Property_Area", StringType(), True),
    StructField("Income", StringType(), True),
    StructField("Application_Status", StringType(), True)
])

# Multi-line variables to use as queries for mySQL database table creation in the main program
query_create_branch_table = """
CREATE TABLE IF NOT EXISTS CDW_SAPP_BRANCH (
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
    TRANSACTION_VALUE DECIMAL(10, 2),
    TIMEID VARCHAR(255),
    CUST_CC_NO VARCHAR(255),
    CUST_SSN INT,
    BRANCH_CODE INT
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
query_create_loan_table = """
CREATE TABLE IF NOT EXISTS CDW_SAPP_loan_application (
    Application_ID VARCHAR(255) PRIMARY KEY,
    Gender VARCHAR(255),
    Married VARCHAR(255),
    Dependents VARCHAR(255),
    Education VARCHAR(255),
    Self_Employed VARCHAR(255),
    Credit_History VARCHAR(255),
    Property_Area VARCHAR(255),
    Income VARCHAR(255),
    Application_Status VARCHAR(255)
); """