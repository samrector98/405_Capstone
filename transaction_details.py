from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# This module includes all transaction-related functions that will be available for the user.

# Set up the spark session
spark = SparkSession.builder.appName("cdw_sapp").getOrCreate()


def get_transactions_by_zip_code():
    zip_code = input("Please enter the zip code: ").strip()

    # Input Validation for Zip Code
    while (zip_code.isnumeric() == False or len(zip_code) != 5):
        print("Error. Your input was invalid. Please enter the 5 digit number with no dashes or spaces.")
        zip_code = input("Please enter the zip code: ").strip()

    date = input("Please enter a date in 'YYYYMM' format: ").strip()

    # Input Validation for Date (Year and Month)
    while (date.isnumeric() == False or len(date) != 6):
        print("Error. Your input was invalid. Please enter the 6 digit number with no dashes or spaces.")
        date = input("Please enter a date in 'YYYYMM' format: ").strip()

    query = """
        SELECT cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE, cr.CUST_CC_NO, cr.CUST_SSN, cr.BRANCH_CODE, cr.TIMEID
        FROM credit_table cr JOIN customer_table cu ON cr.CUST_CC_NO = cu.CREDIT_CARD_NO 
        WHERE cu.CUST_ZIP = '{}' AND cr.TIMEID LIKE '{}%'
        ORDER BY cr.TIMEID DESC
        """.format(zip_code, date)
    
    results = spark.sql(query)

    results_num = results.count()

    # Display the results
    if results_num == 0:
        print("There were no transactions found for that zip code.")
    else:
        results.show()


def get_transactions_by_type():
    type = input("Please enter the type of transaction to search for: ").strip().capitalize()
    
    # Input Validation for Transaction Type
    while type.isalpha() == False:
        print("Error. Please only use alphabetical characters when entering the transaction type: ")
        type = input("Please enter the type of transaction to search for: ").strip().capitalize()

    query = "SELECT * FROM credit_table WHERE TRANSACTION_TYPE = '{}' ORDER BY TIMEID DESC".format(type)
    
    results = spark.sql(query)

    results_num = results.count()

    # Display the results
    if results_num == 0:
        print("There were no transactions found for that type.")
    else:
        results_total = round(results.agg(sum(results['TRANSACTION_VALUE'])).collect()[0][0], 2)

        print("The number of transactions found for the chosen type was {}.".format(results_num))
        print("The total value of the transactions found for the chosen type was ${}.".format(results_total))


def get_transactions_by_state():
    state = input("Please enter the state abbreviation: ").strip().upper()

    # Input Validation for Branch State
    while (state.isalpha() == False or len(state) != 2):
        state = input("Error. Please enter the 2-letter abreviation for the state: ").strip().upper()
    
    query = """
        SELECT cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE, cr.CUST_CC_NO, cr.CUST_SSN, cr.BRANCH_CODE, br.BRANCH_STATE, cr.TIMEID
        FROM credit_table cr JOIN branch_table br ON cr.BRANCH_CODE = br.BRANCH_CODE
        WHERE br.BRANCH_STATE = '{}'
        ORDER BY cr.TIMEID DESC
    """.format(state)
    
    results = spark.sql(query)

    results_num = results.count()

    # Display the results
    if results_num == 0:
        print("There were no transactions found for that state.")
    else:
        results_total = round(results.agg(sum(results['TRANSACTION_VALUE'])).collect()[0][0], 2)

        print("The number of transactions found for the chosen state was {}.".format(results_num))
        print("The total value of the transactions found for the chosen state was ${}.".format(results_total))