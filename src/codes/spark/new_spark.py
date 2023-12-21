import os
import re
from datetime import datetime
from .example_1 import convert_spark_2x_to_3x
from .trim import trim_convert_spark_2x_to_3x
from .extract import extract_convert_spark2x_to_spark3x
from .percentile import percent_convert_spark2x_to_spark3x

from .groupBy import groupby_convert_spark2x_to_spark3x
from .unionpy import union_convert_spark2x_to_spark3x


def convert_spark_code(input_path, output_path, new_prefix, old_prefix):

    output_paths = []  # List to store the paths of the generated output files

    try:
        for filename in os.listdir(input_path):
            if filename.endswith('.py'):
                file_path = os.path.join(input_path, filename)

                # Read the existing Spark 2.x code
                with open(file_path, 'r') as file:
                    spark2x_code = file.read()
                spark3x_code = None
                # Perform the conversion on the Spark 2.x code
                if filename == 'old_example1.py':
                    spark3x_code = convert_spark_2x_to_3x(spark2x_code)
                elif filename == 'old_trim.py':
                    spark3x_code = trim_convert_spark_2x_to_3x(spark2x_code)
                elif filename == 'old_extract.py':
                    spark3x_code = extract_convert_spark2x_to_spark3x(spark2x_code)
                elif filename == 'old_percentile.py':
                    spark3x_code = percent_convert_spark2x_to_spark3x(spark2x_code)
                elif filename == 'old_groupBy.py':
                    spark3x_code = groupby_convert_spark2x_to_spark3x(spark2x_code)
                elif filename == 'old_unionInput.py':
                    spark3x_code = union_convert_spark2x_to_spark3x(spark2x_code)
                # elif filename in ['old_unionInput.py', 'old_sparkGroupByInput.py']:
                #     spark3x_code = process_python(input_path, output_path, new_prefix, old_prefix)
                else:
                    pass

                # Construct the full output file path in the output_path directory
                timestamp_str = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
                output_file_name = f"{new_prefix}_{filename[len(old_prefix):-3]}_{timestamp_str}.py"
                output_file_path = os.path.join(output_path, output_file_name)
                spark3x_code = remove_comments(spark3x_code)

                # Save the converted Spark 3.x code to the output file
                with open(output_file_path, 'w') as output_file:
                    # Remove extra spaces and newlines
                    spark3x_code = '\n\n'.join(line for line in spark3x_code.splitlines() if line.strip())
                    #spark3x_code = "\n".join(line + "\n" for line in spark3x_code.splitlines())

                    output_file.write(spark3x_code)
                    print(f"Processed : {filename}")
                    print(f"Stored: {output_file_name}")

                output_paths.append(output_file_path)

        return output_paths  # Return the list of output file paths

    except Exception as e:
        print(f"Conversion Error:\n{str(e)}")
        return []

def remove_comments(code):
    # Remove comments from the code
    code = re.sub(r'#.*\n', '\n', code)
    return code


