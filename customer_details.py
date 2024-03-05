from pyspark.sql.functions import col, when, sum
from pyspark.sql import SparkSession
import login_info

# This module includes all customer-related functions that will be available for the user.

# Set up the spark session
spark = SparkSession.builder.appName("cdw_sapp").getOrCreate()


def get_customer_details():
    ssn = input("Please enter the customer's social security number: ").strip()
    
    # Input Validation for Credit Card Number
    while (ssn.isnumeric() == False or len(ssn) != 9):
        ssn = input("Error. Your input was invalid. Please enter the 9 digit number with no dashes or spaces: ")

    query = "SELECT * FROM customer_table WHERE SSN = '{}'".format(ssn)

    results = spark.sql(query)

    # Display the results
    if results.count() == 0:
        print("There were no customers found with the specified social security number.")
    else:
        results.show()


update_menu = """
Which part of the customer's information would you like to update?

1. Name
2. Address
3. Phone Number
4. Email Address

Or enter 0 if you are finished updating the information.
"""

name_menu = """
Which part of the name needs to be updated?
            
1. First Name
2. Middle Name
3. Last Name
"""

address_menu = """
Which part of the address needs to be updated?
            
1. Street Address
2. City
3. State
4. Zip Code
5. Country
"""

def update_customer_details(df):
    ssn = input("Please enter the customer's social security number: ").strip()
    
    # Input Validation for Credit Card Number
    while (ssn.isnumeric() == False or len(ssn) != 9):
        print("Error. Your input was invalid. Please enter the 9 digit number with no dashes or spaces.")
        ssn = input("Please enter the customer's social security number: ").strip()

    choice = input(update_menu).strip()

    while choice != '0':
        if choice == '1':
            name_choice = input(name_menu)

            if name_choice == '1':
                first_name = input("Please enter the customer's new first name: ").strip().capitalize()
                
                # Input validation
                while first_name.isalpha() == False:
                    print("Error. Your input was invalid. Please enter the first name with only alphabetical characters.")
                    first_name = input("Please enter the customer's new first name: ").strip().capitalize()

                # Update the dataframe
                df = df.withColumn("FIRST_NAME", when(col("SSN") == ssn, first_name).otherwise(col("FIRST_NAME")))

            elif name_choice == '2':
                middle_name = input("Please enter the customer's new middle name: ").strip().lower()

                # Input validation
                while middle_name.isalpha() == False:
                    print("Error. Your input was invalid. Please enter the middle name with only alphabetical characters.")
                    middle_name = input("Please enter the customer's new middle name: ").strip().lower()

                # Update the dataframe
                df = df.withColumn("MIDDLE_NAME", when(col("SSN") == ssn, middle_name).otherwise(col("MIDDLE_NAME")))
            
            elif name_choice == '3':
                last_name = input("Please enter the customer's new last name: ").strip().capitalize()

                # Input validation
                if last_name.isalpha() == False:
                    print("Error. Your input was invalid. Please enter the last name with only alphabetical characters.")
                    last_name = input("Please enter the customer's new last name: ").strip().capitalize()
            
                # Update the dataframe
                df = df.withColumn("LAST_NAME", when(col("SSN") == ssn, last_name).otherwise(col("LAST_NAME")))
            
            else:
                print("Error. The option you entered was not recognized. Please try again.")
                continue

        elif choice == '2':

            address_choice = input(address_menu)

            if address_choice == '1':
                apt_no = input("Please enter the house or apartment number of the customer's new address: ").strip()

                # Input validation
                while apt_no.isnumeric() == False:
                    print("Error. Your input was invalid. Please enter the house or apt. number with only numerical characters.")
                    apt_no = input("Please enter the house or apartment number of the customer's new address: ").strip()
            
                street_name = input("Please enter the street name of the customer's new address: ")

                full_street = apt_no + " " + street_name

                # Update the dataframe
                df = df.withColumn("FULL_STREET_ADDRESS", when(col("SSN") == ssn, full_street).otherwise(col("FULL_STREET_ADDRESS")))

            elif address_choice == '2':
                city = input("Please enter the city name of the customer's new address: ").strip().capitalize()

                # Update the dataframe
                df = df.withColumn("CUST_CITY", when(col("SSN") == ssn, city).otherwise(col("CUST_CITY")))

            elif address_choice == '3':
                state = input("Please enter the 2-letter abbreviation for the state of the customer's new address: ").strip().upper()

                # Input validation
                while (state.isalpha() == False or len(state) != 2):
                    print("Error. Your input was invalid. Please enter the state abbreviation with 2 alphabetical characters.")
                    state = input("Please enter the 2-letter abbreviation for the state of the customer's new address: ").strip().upper()

                # Update the dataframe
                df = df.withColumn("CUST_STATE", when(col("SSN") == ssn, state).otherwise(col("CUST_STATE")))

            elif address_choice == '4':
                zip_code = input("Please enter the 5-digit zip code of the customer's new address, or enter '99999' if the address does not have a zip code: ").strip()

                # Input validation
                while (zip_code.isnumeric() == False or len(zip_code) != 5):
                    print("Error. Your input was invalid. Please enter the zip code as 5 numerical characters.")
                    zip_code = input("Please enter the 5-digit zip code of the customer's new address, or enter '99999' if the address does not have a zip code: ").strip()

                # Update the dataframe
                df = df.withColumn("CUST_ZIP", when(col("SSN") == ssn, zip_code).otherwise(col("CUST_ZIP")))

            elif address_choice == '5':
                country = input("Please enter the country of the customer's new address: ").strip().capitalize()
                
                # Update the dataframe
                df = df.withColumn("CUST_COUNTRY", when(col("SSN") == ssn, country).otherwise(col("CUST_COUNTRY")))

            else:
                print("Error. The option you entered was not recognized. Please try again.")
                continue

        elif choice == '3':
            phone = input("Please enter the customer's new phone number: ").strip()

            # Input Validation for Customer Phone Number
            while (phone.isnumeric() == False or len(phone) != 10):
                print("Error. Your input was invalid. Please enter the full, 10-digit phone number with no dashes or spaces.")
                phone = input("Please enter the customer's new phone number: ").strip()

            # Once input is validated, re-format the phone number to match the format of phone numbers stored in the dataframe and/or database
            phone = "(" + phone[0:3] + ")" + phone[3:6] + "-" + phone[6:10]

            # Lastly, update the dataframe
            df = df.withColumn("CUST_PHONE", when(col("SSN") == ssn, phone).otherwise(col("CUST_PHONE")))

        elif choice == '4':
            email = input("Please enter the customer's new email address: ").strip()

            # Lastly, update the dataframe
            df = df.withColumn("CUST_EMAIL", when(col("SSN") == ssn, email).otherwise(col("CUST_EMAIL")))

        else:
            print("Error. The option you entered was not recognized. Please try again.")
        
        choice = input(update_menu).strip()

    df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", login_info.mysql_username) \
    .option("password", login_info.mysql_password) \
    .save()
    
    print("The customer's details have been updated in the server.")

    return df


def get_transactions_by_customer_per_month():
    ssn = input("Please enter the customer's social security number: ").strip()
    
    # Input Validation for Credit Card Number
    while (ssn.isnumeric() == False or len(ssn) != 9):
        print("Error. Your input was invalid. Please enter the 9 digit number with no dashes or spaces: ")
        ssn = input("Please enter the customer's social security number: ").strip()
    
    date = input("Please enter a date in 'YYYYMM' format: ").strip()

    # Input Validation for Date (Year and Month)
    while (date.isnumeric() == False or len(date) != 6):
        print("Error. Your input was invalid. Please enter the 6 digit number with no dashes or spaces.")
        date = input("Please enter a date in 'YYYYMM' format: ").strip()

    query = """
        SELECT cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE, cr.TIMEID
        FROM credit_table cr JOIN customer_table cu ON cr.CUST_CC_NO = cu.CREDIT_CARD_NO 
        WHERE cu.SSN = '{}' AND cr.TIMEID LIKE '{}%'
        ORDER BY cr.TIMEID DESC
        """.format(ssn, date)
    
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
    ssn = input("Please enter the customer's social security number: ").strip()

    # Input Validation for Credit Card Number
    while (ssn.isnumeric() == False or len(ssn) != 9):
        print("Error. Your input was invalid. Please enter the 9 digit number with no dashes or spaces: ")
        ssn = input("Please enter the customer's social security number: ").strip()

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
        WHERE cu.SSN = '{}' AND cr.TIMEID BETWEEN '{}' AND '{}'
        ORDER BY cr.TIMEID DESC
        """.format(ssn, start_date, end_date)
    
    results = spark.sql(query)

    # Display the results
    if results.count() == 0:
        print("There were no transactions found for this customer in the selected timeframe.")
    else:
        results.show()