import pandas as pd
import json
import xml.etree.ElementTree as ET
import os
from datetime import datetime

# Directory containing the extracted files
# extract_dir = '/mnt/data/extracted_files'
extract_dir = './Data'
# Logging function
def log_event(phase):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {phase} phase completed.\n"
    with open("./Log/etl_log.txt", "a") as log_file:
        log_file.write(log_message)

# Extract functions
def extract_csv(file_path):
    return pd.read_csv(file_path)

def extract_json(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # Skip lines that aren't valid JSON
    return pd.DataFrame(data)

def extract_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    columns = [elem.tag for elem in root[0]]
    for record in root:
        data.append({elem.tag: elem.text for elem in record})
    return pd.DataFrame(data, columns=columns)

# Master extract function
def extract_data(file_paths):
    data_frames = []
    for file_path in file_paths:
        if file_path.endswith('.csv'):
            data_frames.append(extract_csv(os.path.join(extract_dir, file_path)))
        elif file_path.endswith('.json'):
            data_frames.append(extract_json(os.path.join(extract_dir, file_path)))
        elif file_path.endswith('.xml'):
            data_frames.append(extract_xml(os.path.join(extract_dir, file_path)))
    combined_data = pd.concat(data_frames, ignore_index=True)
    log_event("Extraction")
    return combined_data

# Transformation function
def transform_data1(df):
    print(df)
    df['Height'] = df['height'] * 0.0254
    df['Weight'] = df['weight'] * 0.453592
    df.drop(columns=['height', 'weight'], inplace=True)
    log_event("Transformation")
    return df

def transform_data(df):
    # print(df)
    # Convert columns to numeric, forcing errors to NaN
    df['Height'] = pd.to_numeric(df['height'], errors='coerce')
    df['Weight'] = pd.to_numeric(df['weight'], errors='coerce')
    
    # Apply unit conversions
    df['Height'] = df['Height'] * 0.0254
    df['Weight'] = df['Weight'] * 0.453592
    
    # Drop original columns
    df.drop(columns=['height', 'weight'], inplace=True)
    
    # Log transformation
    log_event("Transformation")
    return df


# Load function
def load_data(df, output_path='./Output/transformed_data.csv'):
    df.to_csv(output_path, index=False)
    log_event("Loading")

# Execute ETL pipeline
def etl_pipeline():
    file_paths = os.listdir(extract_dir)
    raw_data = extract_data(file_paths)
    # print(raw_data)
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)

# Run the ETL pipeline
etl_pipeline()
