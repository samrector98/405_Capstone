from pyspark.sql.functions import col, when, sum
from pyspark.sql import SparkSession
import login_info

# This module includes all customer-related functions that will be available for the user.

# Set up the spark session
spark = SparkSession.builder.appName("cdw_sapp").getOrCreate()


def get_customer_details():
    card_num = input("Please enter the customer's credit card number: ").strip()
    
    # Input Validation for Credit Card Number
    while (card_num.isnumeric() == False or len(card_num) != 16):
        card_num = input("Error. Your input was invalid. Please enter the 16 digit number with no dashes or spaces: ")

    query = "SELECT * FROM customer_table WHERE CREDIT_CARD_NO = '{}'".format(card_num)

    results = spark.sql(query)

    # Display the results
    if results.count() == 0:
        print("There were no customers found with the specified credit card number.")
    else:
        results.show()


def update_customer_details(df):
    card_num = input("Please enter the customer's credit card number: ").strip()
    
    # Input Validation for Credit Card Number
    while (card_num.isnumeric() == False or len(card_num) != 16):
        print("Error. Your input was invalid. Please enter the 16 digit number with no dashes or spaces.")
        card_num = input("Please enter the customer's credit card number: ").strip()

    phone_num = input("Please enter the customer's new phone number: ").strip()

    # Input Validation for Customer Phone Number
    while (phone_num.isnumeric() == False or len(phone_num) != 10):
        print("Error. Your input was invalid. Please enter the full, 10-digit phone number with no dashes or spaces.")
        phone_num = input("Please enter the customer's new phone number: ").strip()

    # Once input is validated, re-format the phone number to match the format of phone numbers stored in the dataframe and/or database
    phone_num = "(" + phone_num[0:2] + ")" + phone_num[3:5] + "-" + phone_num[6:9]

    # Lastly, update the dataframe
    df = df.withColumn("CUST_PHONE", when(col("CREDIT_CARD_NO") == card_num, phone_num).otherwise(col("CUST_PHONE")))

    df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()
    
    print("The customer's details have been updated.")

    return df


def get_transactions_by_customer_per_month():
    card_num = input("Please enter the credit card number: ").strip()
    
    # Input Validation for Credit Card Number
    while (card_num.isnumeric() == False or len(card_num) != 16):
        card_num = input("Error. Your input was invalid. Please enter the 16 digit number with no dashes or spaces: ")
    
    date = input("Please enter a date in 'YYYYMM' format: ").strip()

    # Input Validation for Date (Year and Month)
    while (date.isnumeric() == False or len(date) != 6):
        print("Error. Your input was invalid. Please enter the 6 digit number with no dashes or spaces.")
        date = input("Please enter a date in 'YYYYMM' format: ").strip()

    query = """
        SELECT cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE, cr.TIMEID
        FROM credit_table cr JOIN customer_table cu ON cr.CUST_CC_NO = cu.CREDIT_CARD_NO 
        WHERE cu.CREDIT_CARD_NO = '{}' AND cr.TIMEID LIKE '{}%'
        ORDER BY cr.TIMEID DESC
        """.format(card_num, date)
    
    results = spark.sql(query)

    # Display the results
    if results.count() == 0:
        print("There were no transactions found for this customer in the selected month.")
    else:
        results_num = results.count()
        results_total = round(results.agg(sum(results['TRANSACTION_VALUE'])).collect()[0][0], 2)

        print("You had {} transactions this month, totaling ${}. Here is your monthly statement:".format(results_num, results_total))
        results.show()


def get_transactions_by_customer_in_timeframe():
    card_num = input("Please enter the credit card number: ").strip()

    # Input Validation for Credit Card Number
    while (card_num.isnumeric() == False or len(card_num) != 16):
        card_num = input("Error. Your input was invalid. Please enter the 16 digit number with no dashes or spaces: ")

    start_date = input("Please enter the start date of the timeframe in YYYYMMDD format: ").strip()

    # Input Validation for Start Date
    while (start_date.isnumeric() == False or len(start_date) != 8):
        print("Error. Your input was invalid. Please enter the 8 digit number with no dashes or spaces.")
        start_date = input("Please enter the start date of the timeframe in 'YYYYMMDD' format: ").strip()

    end_date = input("Please enter the end date of the timeframe, in 'YYYYMMDD' format: ").strip()

    # Input Validation for End Date
    while (end_date.isnumeric() == False or len(end_date) != 8):
        print("Error. Your input was invalid. Please enter the 8 digit number with no dashes or spaces.")
        end_date = input("Please enter the end date of the timeframe in 'YYYYMMDD' format: ").strip()

    query = """
        SELECT cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE, cr.TIMEID
        FROM credit_table cr JOIN customer_table cu ON cr.CUST_CC_NO = cu.CREDIT_CARD_NO 
        WHERE cu.CREDIT_CARD_NO = '{}' AND cr.TIMEID BETWEEN '{}' AND '{}'
        ORDER BY cr.TIMEID DESC
        """.format(card_num, start_date, end_date)
    
    results = spark.sql(query)

    # Display the results
    if results.count() == 0:
        print("There were no transactions found for this customer in the selected timeframe.")
    else:
        results.show()