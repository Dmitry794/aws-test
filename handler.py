from __future__ import print_function

import boto3
import csv

import logging

print('Loading function')


def put_item(item):
    table_name = 'test'
    db = boto3.resource('dynamodb', region_name='us-east-2')
    table = db.Table(table_name)
    table.put_item(Item=item)


def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    res = ''
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('input-bucket-csv')
        content = bucket.Object('shopee_order.csv').get()['Body'].read().splitlines()
        reader = csv.DictReader(content)
        for row in reader:
            data = {k: v for k, v in row.items() if v}
            put_item(data)
            res += str(data)

    except Exception as e:
        print(e)
        res = str(e)
        raise e

    response = {
        "statusCode": 200,
        "body": res
    }
    return response
