import os
import json
from datetime import datetime
from time import sleep
import random


def generate_record():
    return {
        "datetime": datetime.now().isoformat(),
        "sales": {
            "quantity": random.randint(1, 10),
            "total_price": abs(round(random.random(), 2) * 10)
        },
        "analytics": {
            "clicks": random.randint(1, 10),
            "impressions": random.randint(10, 20)
        }
    }


def store_json_data(path, data):
    with open(path, "w") as json_file:
        json.dump(data, json_file)


def create_missing_folders(directory):
    import os
    if not os.path.exists(directory):
        os.makedirs(directory)


def main():
    records_list = []
    counter = 0
    while True:
        records_list.append(generate_record())
        if len(records_list) == 10:
            path = os.path.join("data", "json_files", f"file_{counter}.json")
            print(f"writing data to {path}")
            store_json_data(path=path, data=records_list)
            records_list = []
            counter += 1
            sleep(5)


if __name__ == '__main__':
    create_missing_folders(directory=os.path.join("data", "json_files"))
    create_missing_folders(directory=os.path.join("data", "csv_files"))
    create_missing_folders(directory=os.path.join("data", "parquet_files"))
    main()
