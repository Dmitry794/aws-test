from __future__ import print_function

import boto3
import ast
import data_handlers as dh

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

raw_table_name = 'raw_orders'
mapped_table_name = 'mapped_orders'
region_name = 'us-east-2'
input_s3_bucket = 'input-data-test-111620171510'


def ddb_write(table_name, items):
    db = boto3.resource('dynamodb', region_name=region_name)
    table = db.Table(table_name)
    logger.info('Put {} items to {} table'.format(str(len(items)), table_name))
    for item in items:
        item = {k: v for k, v in item.items() if v}  # remove empty values
        table.put_item(Item=item)


def invoke_data_handler(content, data_type):
    result = None
    try:
        handler_name = 'dh.' + data_type + '_handler'
        logger.info('Invoke {}'.format(handler_name))
        handler = eval(handler_name)
        result = handler(content)
    except (NameError, AttributeError):
        logger.error('Handler for {} not found'.format(data_type))
    return result


def file_handler(event, context):
    logger.info('start file handler')
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(input_s3_bucket)
        for record in event['Records']:
            filename = record['s3']['object']['key'].encode('ascii')
            if not filename.endswith('/'):  # exclude folders
                data_type = filename.split('.')[-1]
                logger.info('read file: ' + filename)
                content = bucket.Object(filename).get()['Body'].read().splitlines()
                items = invoke_data_handler(content, data_type)
                if items is not None:
                    ddb_write(raw_table_name, items)
    except Exception as e:
        logger.error(e)
        raise e


def items_handler(event, context):
    logger.info('start items handler')
    try:
        records = event['Records']
        for record in records:
            event_name = record['eventName']
            if event_name == 'INSERT':
                keys = record['dynamodb']['NewImage']['items']['S']
                items = ast.literal_eval(keys.encode('ascii'))
                ddb_write(mapped_table_name, items)
    except Exception as e:
        logger.error(e)
        raise e
