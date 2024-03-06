# README
This program was created as a submission for my capstone project in a data analyst training course. 


## Scenario
The scenario for this project involves making a database for a banking company. The data to be made into a database was initially provided as three seperate JSON files, each representing what would become one of the tables in the database. After loading the data and modifying/cleaning it to match the company's business requirements for the database, the database is created and populated with the data. Additionally, a fourth table is created in the database, this time from an API source. Once the database is created, the program will allow a user to perform several operations involving input to query the database, and then returning the results.

## Notes
**cdw_sapp.py** is the file to run the program. All other .py files are used through imports.

The program references the information to login to a SQL server in the **login_info.py** file.
For the most ease of use, please make a file of this name, and include the following information...
    The server address for the mySQL server in the variable **mysql_host**
    The username for the mySQL account in the variable **mysql_username**
    The password for the mySQL account in the variable **mysql_password**
NOTE: Make sure to **git.ignore** this file if you plan on sharing your own version of the program!!!

The JSON data files are the input initially provided to us for the project.
The CSV data files are the final version of the cleaned data, to be used in Power BI visualizations.

# Report

## Loading the Database
Loading the data was not as simple as just using the built in functions in PySpark, as the intial versions of the file were not in proper JSON format. Since each line in the file had one JSON object / entry of the data, I fixed this by simply making a list of all the lines, and then using the built-in **createDataFrame()** function in addition to my own custom-defined schema for each of the tables.

Once the data was in their respective dataframes, I performed data modification and cleaning as specified in the mapping document. The functions that carry out the process for each of the tables are in **database_setup.py**.

I initially used mySQL connector to connect to the server and then create the database for this project, as well as the empty tables for the data. Later, after the above parts, I used the PySpark **spark.write()** function to load the data from each dataframe into their respective tables in the mySQL database.

## Access to LOAN API Endpoint
I loaded the final data table from the API endpoint provided to us. The program will check this source every time it runs, so there will be no problem if the data changes in the future. This is done by using the **requests.get()** built-in function from the **requests** module. I also have it print out the status code recieved from the API.

## Application Front-End
For the front end of the application, I simply made a loop that continues until the user enters the option to quit the program. For each stage, a multi-line menu string is shown to the user, and the user is asked to input the number of the operation they'd like to perform, as displayed in each menu. If the user inputs a value is either non-numeric, or does not match one of the values specified in the menu, the program notifies the user that they made an error and prompts them to input again.

The first three options of the main menu involve **transaction details**. The functions used for these operations are stored in **transactions_details.py**. Each of the functions prompts the user for the necessary input information, and performs input validation before allowing the function to continue. If the input is not valid, it simply prompts the user to input valid information, while providing distinctions for valid data, such as the number of characters. 
    **get_transactions_by_zip_code()** gets user input for the zip code and a date in year/month (YYYYMM) format, and prints all matching entries from the relevant table in the database.
    **get_transactions_by_type()** gets user input for a type of transaction, gets all matching entries from the relevant table, and then prints out the count and total value of the collected transactions.
    **get_transactions_by_state()** gets user input for the 2-letter abbreviation of one of the 50 states in the USA, gets all matching entries from the relevant table, and then prints out the count and total value of the collected transactions.

The last four options of the main menu involve **customer details**. The functions used for these operations are stored in **customer_details.py**. As with the previous module, Each of the functions prompts the user for the necessary input information, and performs input validation before allowing the function to continue. If the input is not valid, it simply prompts the user to input valid information, while providing distinctions for valid data, such as the number of characters. 
    **get_customer_details()** has the user input a social security number, and then returns the matching customer entry from the database.
    **update_customer_details()** also has the user specify a social security number, but then prompts the user with several nested menus in order to determine what aspect of the customer's details needs to be updated, and then prompts the user for the new value for the specified detail. Each option has their own input validation and error handling to ensure no misformatted or faulty data is sent to the database.
    **get_monthly_card_summary()** prompts the user for a credit card number and a date in YYYY/MM format, and gets all the matching records from the database. After this, it prints out the number of transactions and the total value of the transactions before then generating a list of all the transactions for the user to view.
    **get_transactions_by_customer()** prompts the user to enter a customer's social security number and then a start and end date, both in YYYYMMDD format. It then gets all matching records from the database, and displays them to the user in chronological order, with the newest transactions first.

## Data Analysis and Visualization
For each requested visualization, I picked the type that best fit the requested information, and then included extra information.
    Example: When asked for the % value of a option, I used a pie chart to display the requested option as well as the % of all other options for that variable.
    Example: If asked for the highest value of a variable, I made a bar chart showing the top 'X' values, in addition to the requested top value.
To create the visualizations, I created any measures I needed for each visualization, and then used the filters to create the specific results.