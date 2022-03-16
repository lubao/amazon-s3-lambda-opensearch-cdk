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
    #print(event)
    try:
        for _record in event['Records']:
            if _record['eventSource'] == 'aws:s3':
                _handle_s3_event(_record)
            elif _record['eventSource'] == 'aws:sqs':
                _messages = json.loads(_record['body'])
                if 's3:TestEvent'== _messages.get('Event', False):
                    continue
                for _message_event in _messages['Records']:
                    _handle_s3_event(_message_event, bulk=True)
            else:
                raise Exception('Unsuuported Event Source')
    except Exception as e:
        print(f'Exception: {e}')
        print(f'Event Received: {event}')
        raise e
    return

def _handle_s3_event(record, bulk=False):
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
    # _handle_s3_event(_record)
    return

def _upload_documents(bucket, key, bulk=False):
    try:      
        with tempfile.TemporaryDirectory() as _dir:
            _file_path = os.path.join(_dir, key.split('/')[-1])
            S3_CLIENT.download_file(bucket, key, _file_path)
            _body = []
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
                        _act = json.dumps({'index':{'_index':_index}})
                        _body.append(f'{_act}\n{_data}\n')
                        # _body.append(
                        #     json.dumps({'index':{'_index':_index}})
                        # )
                        # _body.append(_data)
                    else:
                        _res = DOMAIN_CLIENT.upload_document(
                            index=_index,
                            data=_data
                        )
                #        print(f'Response: {_res.status_code} {_res.text}')
                if bulk:
                        # _body.append('\n')
                        _res = DOMAIN_CLIENT.bulk_upload_document(
                            index=_index,
                            data=''.join(_body)
                        )
                
                print(f'Response: {_res.status_code} {_res.text}')

    except Exception as e:
        print(f'Exception: {e}')
        raise e
    return