import json

# Open the first input file, which includes the data about the company's offices and locations.
input_file_one = open("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/input_files/cdw_sapp_branch.json")
branch_data = []
for line in input_file_one:
    entry = json.loads(line)
    branch_data.append(entry)
#print(type(branch_data))
#print(len(branch_data))
#print(branch_data[0])
#print(type(branch_data[0]))

# Open the second input file, which includes the data about each credit card, its owner, and its transactions.
input_file_two = open("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/input_files/cdw_sapp_credit.json")
credit_data = []
for line in input_file_two:
    entry = json.loads(line)
    credit_data.append(entry)
#print(type(credit_data))
#print(len(credit_data))
#print(credit_data[0])
#print(type(credit_data[0]))

# Open the third input file, which includes the data about each customer.
input_file_three = open("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/input_files/cdw_sapp_customer.json")
customer_data = []
for line in input_file_three:
    entry = json.loads(line)
    customer_data.append(entry)
#print(type(customer_data))
#print(len(customer_data))
#print(customer_data[0])
#print(type(customer_data[0]))