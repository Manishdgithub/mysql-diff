import parse
import argparse
import re
from pprint import pprint
import os
import sqlparse

def same_params(params1, params2):
    try:
        list1 = params1.split()
        list2 = params2.split()
        if len(list2) == len(list1):
            for idx, val in enumerate(list1):
                if list2[idx] != val:
                    return False
        else:
            return False
        return True
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        exit()

def compare_two_dicts_and_return_alter(db_dict1, db_dict2):
    try:
        output_sql = ''
        for key, value in db_dict1.items():
            if key not in db_dict2:
                # No such table in db_dict2, create table
                output_temp = []
                engine_type = value.pop('ENGINE', 'InnoDB')  # Default to InnoDB if not specified
                for i, (key2, value2) in enumerate(value.items()):
                    if i == len(value.items()) - 1:
                        output_temp.append(f'`{key2}` {value2}')
                    else:
                        output_temp.append(f'`{key2}` {value2},')
                output_sql += f'\nCREATE TABLE `{key}` ({"".join(output_temp)}) ENGINE={engine_type};\n'
            else:
                # Table exists, compare fields
                for key2, value2 in value.items():
                    if key2 not in db_dict2[key]:
                        # Field does not exist, add field
                        output_sql += f'ALTER TABLE `{key}` ADD `{key2}` {value2};\n'
                    elif not same_params(value2, db_dict2[key][key2]):
                        # Field exists but parameters differ, modify field
                        output_sql += f'ALTER TABLE `{key}` MODIFY `{key2}` {value2};\n'
        return output_sql.replace("`", "")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        exit()

def parse_db_to_dict(db_string=''):
    try:
        temp_dict = {}
        # Updated regex pattern to match both InnoDB and MyISAM
        for table in re.findall("CREATE TABLE `(.+?)` \((.+?)\) ENGINE=(InnoDB|MyISAM)", db_string, re.DOTALL):
            # table[0] = tablename
            # table[1] = all table fields
            # table[2] = engine type (InnoDB or MyISAM)
            temp_table_dict = {}
            print(f"Table: {table[0]}")
            table_lines = table[1].split(",\n")
            for line in table_lines:
                # Remove leading and trailing spaces
                line = line.strip()
                # Skip empty lines
                if not line:
                    continue
                # Split the line into field name and field description
                parts = line.split(" ", 1)
                field_name = parts[0].strip("`")
                field_description = parts[1] if len(parts) > 1 else ''
                temp_table_dict[field_name] = field_description
                print(f"Field Name: {field_name}, Field Description: {field_description}")
            temp_dict[table[0]] = temp_table_dict
        return temp_dict
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        exit()

def compare_and_write_diff_to_file(path1, path2, path3):
    try:
        # Read content from path1
        with open(path1, 'r', encoding="utf-8") as myfile:
            db1_string = myfile.read()
        # Read content from path2
        with open(path2, 'r', encoding="utf-8") as myfile:
            db2_string = myfile.read()
        # Parse content to dictionaries (assuming parse_db_to_dict is defined elsewhere)
        db1_dict = parse_db_to_dict(db1_string)
        db2_dict = parse_db_to_dict(db2_string)
        # Compare dictionaries and return the difference (assuming compare_two_dicts_and_return_alter is defined elsewhere)
        diff_sql_alter = compare_two_dicts_and_return_alter(db1_dict, db2_dict).replace(",,", ",")
        diff_sql_alter = diff_sql_alter.replace(",;", ";")
        #to pretify sql query
        #diff_sql_alter = sqlparse.format(diff_sql_alter, reindent=True, keyword_case='upper')
        #print(f"diff_sql_alter after=========={diff_sql_alter}")
        # Write the difference to path3
        with open(path3, 'w', encoding="utf-8") as f:
            print(diff_sql_alter, file=f)
        print(diff_sql_alter)
        print(f"Successfully Create the delta file:{path3}")
    except Exception as e:
        print(f"Error: {str(e)}")
        exit()

def validate_user_input(*args):
    try:
        for arg in args:
            if arg is None or arg == "":
                raise ValueError("All arguments must be non-empty.")
            else:
                print(f'arg: {arg}')
        return True
    except ValueError as e:
        print(f"Error: {e}")
        return False

def user_input_args():
    try:
        parser = argparse.ArgumentParser(description='Find diff in two MySQL dumps and create diff file with ALTER commands(like migration')
        parser.add_argument('db_file1', type=str, nargs=1, help='dbdump1')
        parser.add_argument('db_file2', type=str, nargs=1, help='dbdump2')
        parser.add_argument('output_file', type=str, nargs=1, help='output file')
        args = parser.parse_args()
        args = vars(args)
        param_1 = args['db_file1'][0]
        param_2 = args['db_file2'][0]
        param_3 = args['output_file'][0]
        validate_user_input(param_1, param_2, param_3)
        return param_1, param_2, param_3
    except IndexError:
        print("Error: Not enough command line arguments provided.")
        print("Usage: python EnlivenDBMigration.py 'base_script_flag' 'latest_sha_id' 'latest_version'")

if __name__ == '__main__':
    path1,path2,path3 = user_input_args()
    compare_and_write_diff_to_file(path1, path2, path3)
