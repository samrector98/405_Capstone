import json

def load_data_from_file(file_name):
    input_file = open(file_name)
    data = []
    for line in input_file:
        entry = json.loads(line)
        data.append(entry)
    
    input_file.close()
    return data

branch_data = load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/input_files/cdw_sapp_branch.json")
print(type(branch_data))
print(len(branch_data))
print(branch_data[0])
print(type(branch_data[0]))

credit_data = load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/input_files/cdw_sapp_credit.json")
print(type(credit_data))
print(len(credit_data))
print(credit_data[0])
print(type(credit_data[0]))

customer_data = load_data_from_file("C:/Users/fuzed/Desktop/Per_Scholas/405_CapstoneProject/RTT89_M405/input_files/cdw_sapp_customer.json")
print(type(customer_data))
print(len(customer_data))
print(customer_data[0])
print(type(customer_data[0]))