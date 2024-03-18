import xml.etree.ElementTree as ET
import json
import csv
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet Files") \
    .getOrCreate()

def flatten_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    data = []
    for record in root.findall('record'):
        record_data = {}
        for child in record:
            record_data[child.tag] = child.text
        data.append(record_data)
    return data

def flatten_json(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
    return data

json_path = "./Files/transactions.json"
xml_path = "./Files/products.xml"


json_flattened_data = flatten_json(json_path)

xml_flattened_data = flatten_xml(xml_path)

with open('./Dest_Files/transactions.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=json_flattened_data[0].keys())
    writer.writeheader()
    writer.writerows(json_flattened_data)

with open('./Dest_Files/products.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=xml_flattened_data[0].keys())
    writer.writeheader()
    writer.writerows(xml_flattened_data)




