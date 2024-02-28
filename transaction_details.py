from pyspark.sql import SparkSession

# Set up the spark session
spark_app = SparkSession.builder.appName("sparkdemo").getOrCreate()

def get_transactions_by_zip_code():
    zip_code = input("Please enter the zip code: ")
    year = input("Please enter the year: ")
    month = input("Please enter the month (number): ")

    date = year + month

    query = """
        SELECT cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE, cr.CUST_CC_NO, cr.CUST_SSN, cr.BRANCH_CODE, cr.TIMEID
        FROM credit_table cr JOIN customer_table cu ON cr.CUST_CC_NO = cu.CREDIT_CARD_NO 
        WHERE cu.CUST_ZIP = '{}' AND cr.TIMEID LIKE '{}%'
        ORDER BY cr.TIMEID DESC
        """.format(zip_code, date)
    
    results = spark_app.sql(query)

    return results


def get_transactions_by_type():
    type = input("Please enter the type of transaction to search for: ")

    query = "SELECT * FROM credit_table WHERE TRANSACTION_TYPE = '{}' ORDER BY TIMEID DESC".format(type)
    
    results = spark_app.sql(query)

    return results


def get_transactions_by_state():
    state = input("Please enter the state abbreviation: ")
    
    query = """
        SELECT cr.TRANSACTION_ID, cr.TRANSACTION_TYPE, cr.TRANSACTION_VALUE, cr.CUST_CC_NO, cr.CUST_SSN, cr.BRANCH_CODE, br.BRANCH_STATE, cr.TIMEID
        FROM credit_table cr JOIN branch_table br ON cr.BRANCH_CODE = br.BRANCH_CODE
        WHERE br.BRANCH_STATE = '{}'
        ORDER BY cr.TIMEID DESC
    """.format(state)
    
    results = spark_app.sql(query)

    return results