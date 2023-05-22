from .hive import hive_new_path
from .hive import hive_rem_param
from .hive import hive_update_table_name
from .spark import spark_new_path
from .spark import spark_rem_param
import os
import re

def main():
    global result;

    input_path = "C:/Users/SCHILLAL/PycharmProjects/Accelerator/Media/user_input/"
    new_path = "C:/Users/SCHILLAL/PycharmProjects/Accelerator/Media/destination_folder/"
    os.chdir(input_path)

    with os.scandir(input_path) as directory:
        for item in directory:
            new_item = str(item).split("'")[1].replace("old", "new")
            print(new_item)

            if not item.name.startswith('.') and item.is_file():
                with open(item, mode="r+") as file:
                    script_data = file.read()

                    # for line in script_data:
                    script_data = re.sub(' +',' ',script_data)
                    script_data = re.sub('\n+', '\n', script_data)

                    script_updated_path = hive_new_path.changepath(script_data)
                    script_updated_param = hive_rem_param.removeUnwantedParameters(script_updated_path)
                    script_updated_table = hive_update_table_name.updateTableName(script_updated_param)

                    file.close()

                    with open(new_path + new_item, 'w') as file:
                        file.write(script_updated_table)

                    os.remove(item)


    # with open('C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/old_cdh_script/spark/spark_loaddata.sh', 'r') as file:
    #     spark_script = file.readline().strip()
    #
    # with open('C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/old_cdh_script/spark/old_cdh_spark_dataprocessing.txt', 'r') as file:
    #     spark_script2 = file.read()
    #

    #
    # updated_spark_path = spark_new_path.update_spark_path(spark_script)
    # updated_spark_param = spark_rem_param.remove_hortonworks_imports(spark_script2)
    #
    # with open('C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/new_cdp_script/spark/new_spark_loaddata.sh','w') as file:
    #     file.write(updated_spark_path)
    #     file.close()
    #
    # with open('C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/new_cdp_script/spark/new_spark_loadhdfs.txt','w') as file:
    #     file.write(updated_spark_param)
    #     file.close()



main()