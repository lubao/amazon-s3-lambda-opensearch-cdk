#!/usr/bin/env python3
import os
import boto3
import tempfile
from domainclient import DomainClient
from datetime import datetime, date
import json

S3_CLIENT = boto3.client('s3')
DOMAIN_CLIENT = DomainClient(endpoint=os.environ['DOMAIN_ENDPOINT'])
INDEX_PREFIX = os.environ['INDEX_PREFIX']

def handler(event, context):
    try:
        for _record in event['Records']:
            # Handle S3 event
            if _record['eventSource'] == 'aws:s3':
                _handle_s3_event(_record)
            elif _record['eventSource'] == 'aws:sqs':
                # Handle S3 to SQS event
                _messages = json.loads(_record['body'])
                if 's3:TestEvent'== _messages.get('Event', False):
                    continue
                for _message_event in _messages['Records']:
                    _handle_s3_event(_message_event)
            else:
                raise Exception('Unsuuported Event Source')
    except Exception as e:
        print(f'Exception: {e}')
        print(f'Event Received: {event}')
        raise e
    return

def _handle_s3_event(record, bulk=True):
    if 'ObjectCreated' not in record['eventName']:
        print(f"Non supported S3 event: {record['eventName']}")
        return
    if record['s3']['object']['size'] == 0:
        # Skip Zero size object
        return
    _upload_documents(
        bucket = record['s3']['bucket']['name'],
        key = record['s3']['object']['key'],
        bulk=bulk,
    )
    return

def _upload_documents(bucket, key, bulk=True):
    try:      
        with tempfile.TemporaryDirectory() as _dir:
            _file_path = os.path.join(_dir, key.split('/')[-1])
            _bulk_file = os.path.join(_dir, 'bulk')
            S3_CLIENT.download_file(bucket, key, _file_path)
            with open(_file_path, 'r') as _obj:
                for _data in _obj.readlines():
                    _data=_data.strip()
                    try:
                        _ts=json.loads(_data)['timestamp']
                        _dt=datetime.fromtimestamp(int(int(_ts)/1000))
                    except Exception as e:
                        print(f'Load Timestamp Exception: {e}, '
                            'Using current time.'
                        )
                        _dt = date.today()
                    _index=f'{INDEX_PREFIX}-'\
                        f'{_dt.year}-{_dt.month}-{_dt.day}'
                    if bulk:
                        with open(_bulk_file, 'a') as _bulk:
                            _act = json.dumps({'index':{'_index':_index}})
                            _bulk.write(f'{_act}\n')
                            _bulk.write(f'{_data}\n')
                    else:
                        _res = DOMAIN_CLIENT.upload_document(
                            index=_index,
                            data=_data
                        )
                if bulk:
                        with open(_bulk_file, 'rb') as _f:
                            _res = DOMAIN_CLIENT.bulk_upload_document(
                                data= _f.read()
                            )
                    
                print(f'Response: {_res.status_code} {_res.text}')

    except Exception as e:
        print(f'Exception: {e}')
        raise e
    return