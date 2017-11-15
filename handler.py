from __future__ import print_function

import boto3
import csv
import ast

import logging

print('Loading function')


def csv_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        table_name = 'raw_orders'
        db = boto3.resource('dynamodb', region_name='us-east-2')
        table = db.Table(table_name)

        s3 = boto3.resource('s3')
        bucket = s3.Bucket('input-csv')
        content = bucket.Object('shopee_order.csv').get()['Body'].read().splitlines()
        reader = csv.DictReader(content)
        for row in reader:
            data = {k: v for k, v in row.items() if v}  # remove empty values
            table.put_item(Item=data)

    except Exception as e:
        print(e)
        raise e


def stream_handler(event, context):
    print(event)
    table_name = 'mapped_orders'
    db = boto3.resource('dynamodb', region_name='us-east-2')
    table = db.Table(table_name)

    records = event['Records']
    for record in records:
        event_name = record['eventName']
        if event_name == 'INSERT':
            keys = record['dynamodb']['NewImage']['items']['S']
            items = ast.literal_eval(keys.encode('ascii'))
            for row in items:
                item = {k: v for k, v in row.items() if v}  # remove empty values
                table.put_item(Item=item)
                print(item)
