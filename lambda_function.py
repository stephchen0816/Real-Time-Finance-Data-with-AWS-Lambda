import json
import boto3
import random
import datetime
from time import sleep
import yfinance as yf

kinesis = boto3.client('kinesis', "us-east-2")

def lambda_handler(event, context):
    begin = '2023-04-03'
    stop  = '2023-04-14'

    tickers = ['AMZN', 'BABA', 'WMT', 'EBAY', 'SHOP', 'TGT', 'BBY', 'HD', 'COST', 'KR']
    interval = "5m"

    rowcount = 0

    for tick in tickers:
        stock = yf.Ticker(tick)
        data = stock.history(start=begin, end=stop, interval = interval)


        total_data ={} 
        for x,y in data.iterrows():
            kinesis_record = {
                "high":y["High"],
                "low":y["Low"], 
                "volatility": y["High"] - y["Low"],
                "ts":x.strftime('%Y-%m-%d %H:%M:%S'), 
                "name":tick
                }
                
            print(kinesis_record)
            total_data = json.dumps(kinesis_record)+"\n"

            kinesis.put_record(
                    StreamName="kinesis-datastream-project",
                    Data=total_data.encode('utf-8'),
                    PartitionKey="partitionkey")
            sleep(0.05)
            
            rowcount = rowcount + 1
            print(rowcount)
        print(f"The total number of records sent to Kinesis: {rowcount}")       

    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }

