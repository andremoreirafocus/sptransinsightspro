import os
import json


def save_data_to_json_file(json_data, downloads_folder, file_name):
    file_with_path = os.path.join(downloads_folder, file_name)
    print(f"Writing buses_positions to file {file_with_path} ...")
    with open(file_with_path, "w") as file:
        file.write(json.dumps(json_data))
    print("File saved successfully!!!")
