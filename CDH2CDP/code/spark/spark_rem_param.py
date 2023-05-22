import re
import json

def remove_hortonworks_imports(spark_script2):
    with open('C:/Users/SCHILLAL/PycharmProjects/cdh_to_cdp/code/config.json', 'r') as f:
        data = json.load(f)

    remove_lines = data["spark"]["remove"]["import com.hortonworks.spark.sql.hive.llap.{HiveWarehouseBuilder, HiveWarehouseSession}"]

    if(remove_lines):
        lines = spark_script2.split('\n')
        filtered_lines = [line for line in lines if not line.startswith('import com.hortonworks')]
        filtered_text = '\n'.join(filtered_lines)
        return filtered_text





