import requests
import boto3
from datetime import datetime, timedelta
import os
import pandas as pd
import matplotlib.pyplot as plt
import json

def get_water_data():
    # Fetch water level data

    # Using a real USGS gauge - Potomac River at Little Falls, MD (01646500)
    url = "https://waterservices.usgs.gov/nwis/iv/"
    params = {
        'format': 'json',
        'sites': '01646500',  # Potomac River gauge
        'parameterCd': '00065',  # Gage height
        'siteStatus': 'all'
    }
    
    response = requests.get(url, params=params)
    return response.json()

def write_to_dynamodb(data):
    # Write water level data to DynamoDB

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('water-tracking')
    
    # Parse response
    time_series = data['value']['timeSeries'][0]
    values = time_series['values'][0]['value']
    
    for value in values:
        timestamp = value['dateTime']
        water_level = float(value['value'])
        
        table.put_item(Item={
            'station_id': '01646500',
            'timestamp': timestamp,
            'water_level_ft': water_level
        })

def generate_plot_and_upload():
    # Generate plot from DynamoDB and upload to S3
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('water-tracking')
    
    # Query all records
    response = table.scan()
    items = response['Items']
    
    if not items:
        print("No data found")
        return
    
    df = pd.DataFrame(items)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    # Create plot
    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['water_level_ft'].astype(float))
    plt.title('USGS Water Level - Potomac River')
    plt.xlabel('Time')
    plt.ylabel('Water Level (feet)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/tmp/plot.png')
    
    # Save CSV
    df.to_csv('/tmp/data.csv', index=False)
    
    # Upload to S3
    s3 = boto3.client('s3')
    bucket = os.environ['S3_BUCKET']
    s3.upload_file('/tmp/plot.png', bucket, 'plot.png')
    s3.upload_file('/tmp/data.csv', bucket, 'data.csv')
    
    print(f"Uploaded plot and CSV to {bucket}")

def backfill_historical():
    # Fetch last 72 hours of data

    print("Backfilling historical data...")
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=72)
    
    url = "https://waterservices.usgs.gov/nwis/iv/"
    params = {
        'format': 'json',
        'sites': '01646500',
        'parameterCd': '00065',
        'startDT': start_time.strftime('%Y-%m-%dT%H:%M'),
        'endDT': end_time.strftime('%Y-%m-%dT%H:%M')
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    write_to_dynamodb(data)
    print("Backfill complete")

if __name__ == "__main__":
    # Check if table is empty (first run)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('water-tracking')
    
    try:
        response = table.scan(Limit=1)
        if not response['Items']:
            backfill_historical()
    except:
        # Table might not exist yet
        print("Table not found - run create-table first")
    
    # Get latest data
    latest = get_water_data()
    write_to_dynamodb(latest)
    generate_plot_and_upload()