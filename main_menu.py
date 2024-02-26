menu = """ Please select from the following options:

1. Display all transactions in a specific zip code over a specified time frame.
2. Display all transactions for a specific transaction type.
3. Display all transactions from all branches located in a specific state.
4. Display the account details of a specific customer.
5. Modify the account details of a specific customer.
6. Generate a monthly report containing the transactions on a specific credit card.
7. Display all transactions made by a specific customer over a specified time frame.

Or, enter '0' to exit the program.

Your choice: """

user_input = input(menu)

while user_input != '0':

    if user_input == '1':
        # run function
        print()
    elif user_input == '2':
        # run function
        print()
    elif user_input == '3':
        # run function
        print()
    elif user_input == '4':
        # run function
        print()
    elif user_input == '5':
        # run function 5
        print()
    elif user_input == '6':
        # run function 5
        print()
    elif user_input == '7':
        # run function 5
        print()
    else:
        print("Error. The option you entered was not recognized. Please try again.")
    
    user_input = input(menu)

print("Exiting program...") 