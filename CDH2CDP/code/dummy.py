import os
from hive import hive_new_path
from hive import hive_rem_param
from hive import hive_update_table_name
from spark import spark_new_path
from spark import spark_rem_param

input_path = "C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/old_cdh_script/hive"
new_path = "C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/new_cdp_script/hive"
os.chdir(input_path)

file_list = []

for filename in os.listdir(input_path):
    if os.path.isfile(os.path.join(input_path, filename)):
        file_list.append(filename)

print('List of files available : \n')
for file in file_list:
    print(file)

searchString = input('\nEnter file name to search: \n')
# searchString2='etl'
search_list = []

print('List of files with the given search string : \n')
for file in file_list:
    # if searchString in file or searchString2 in file:
    if searchString in file:
        search_list.append(file)
        print(search_list[len(search_list)-1])

        # with os.scandir(input_path) as directory:
        #  for item in directory:
        # new_file = str(file).split("'")[1].replace("old","new")
        for file in search_list:
            new_file = file.replace("old","new")
        #     print(new_item)

        # if not file.name.startswith('.') and file.is_file():
            if not file.startswith('.'):
                with open(file, mode="r+") as file:
                    script_data= file.read()

                script_updated_path = hive_new_path.changepath(script_data)
                script_updated_param = hive_rem_param.removeUnwantedParameters(script_updated_path)
                script_updated_table = hive_update_table_name.updateTableName(script_updated_param)

                with open('C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/new_cdp_script/hive/'+new_file, 'w') as file:
                    file.write(script_updated_table)