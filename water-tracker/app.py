import requests
import boto3
from datetime import datetime, timedelta
import os
import pandas as pd
import matplotlib.pyplot as plt
import json
from decimal import Decimal

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

def write_to_dynamodb(data, table='water-tracking'):
    # Write water level data to DynamoDB

    try:
        time_series = data['value']['timeSeries'][0]
        values = time_series['values'][0]['value']
        
        for value in values:
            timestamp = value['dateTime']
            water_level = float(value['value'])
            
            table.put_item(Item={
                'station_id': '01646500',
                'timestamp': timestamp,
                'water_level_ft': Decimal(str(water_level))
            })
            print(f"Written: {timestamp} - {water_level} ft")
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")

def generate_plot_and_upload(table, bucket):
    # Generate plot from DynamoDB and upload to S3
    
    try:
        # Query all records
        response = table.query(
            KeyConditionExpression='station_id = :sid',
            ExpressionAttributeValues={':sid': '01646500'}
        )
        items = response['Items']
        
        if not items:
            print("No data found in DynamoDB")
            return
        
        df = pd.DataFrame(items)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        df['water_level_ft'] = df['water_level_ft'].astype(float)
    
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
        s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        s3.upload_file('/tmp/plot.png', bucket, 'plot.png')
        s3.upload_file('/tmp/data.csv', bucket, 'data.csv')
        
        print(f"Uploaded plot and CSV to {bucket}")
        print(f"Total records: {len(df)}")
    except Exception as e:
        print(f"Error generating plot: {e}")
def backfill_historical(table):
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
    write_to_dynamodb(data, table)
    print("Backfill complete")

if __name__ == "__main__":
    # Check if table is empty (first run)
    region = os.environ.get('AWS_REGION', 'us-east-1')
    table_name = os.environ.get('DYNAMODB_TABLE', 'water-tracking')
    bucket = os.environ.get('S3_BUCKET', 'ds5220-dp2-vcx4ka')
    
    print(f"Using region: {region} \nTable: {table_name} \nBucket: {bucket}")
    
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    try:
        response = table.query(
            KeyConditionExpression='station_id = :sid',
            ExpressionAttributeValues={':sid': '01646500'},
            Limit=1
        )
        if not response['Items']:
            backfill_historical(table)

        # Get latest data
        latest = get_water_data()
        write_to_dynamodb(latest, table)
        generate_plot_and_upload(table, bucket)
        print("Pipeline run complete!")
    except Exception as e:
        # Table might not exist yet
        print(f"Error checking table and getting data: {e}")
    
