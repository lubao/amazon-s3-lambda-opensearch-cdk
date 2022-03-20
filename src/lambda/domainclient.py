import os
import boto3
import requests
from  requests_aws4auth import AWS4Auth
import json

HTTP_GET = 'GET'
HTTP_POST = 'POST'
HTTP_PUT = 'PUT'
HTTP_PATCH = 'PATCH'
HTTP_HEAD = 'HEAD'
HTTP_DELETE = 'DELETE'

class BaseClient(object):


    def __init__(self, endpoint, **kwargs):
        self._endpoint = \
            endpoint if not endpoint.endswith('/') else endpoint[:-1]
        self._service = 'es'
        # Not retrive ENV variable here
        # since the ENV will be set while deploying
        self._region = None

    def _auth(self):
        credentials = boto3.Session().get_credentials()
        self._region = \
            self._region if self._region else os.environ['AWS_REGION'] 
        return AWS4Auth(
            credentials.access_key, 
            credentials.secret_key, 
            self._region,
            self._service, 
            session_token=credentials.token
        )

    def _make_request(
            self, method, path='', headers=None, data=None, files=None
        ):
        
        _auth = self._auth()
        url = '/'.join([
            self._endpoint,
            path if not path.startswith('/') else path[1:]
        ])
        if method == HTTP_GET:
            ret=requests.get(url, auth=_auth, stream=True)
        elif method == HTTP_HEAD:
            ret=requests.head(url, auth=_auth, stream=True)
        elif method == HTTP_DELETE:
            ret=requests.delete(url, auth=_auth, stream=True)
        elif method == HTTP_PUT:
            ret=requests.put(url, auth=_auth, headers=headers, data=data)
        elif method == HTTP_POST:
            if files is not None:
                ret=requests.post(
                    url, auth=_auth, headers=headers, files=files
                )
            else:
                ret=requests.post(url, auth=_auth, headers=headers, data=data)
        elif method == HTTP_PATCH:
            ret=requests.patch(url, auth=_auth, headers=headers, data=data)
        else:
            raise Exception(f'Unsupported HTTP Verb: {method}')
        if ret.status_code >= 300:
           raise Exception(f'Not OK Status Code: {ret.status_code} {ret.text}')
        # print('Response :'+ ret.text)
        # ret.raise_for_status()
        return ret

class DomainClient(BaseClient):


    def __init__(self, endpoint, **kwargs):
        super().__init__(endpoint, **kwargs)

    def get_domain_info(self):
        res = self._make_request(HTTP_GET)
        return json.loads(res.text)

    def upload_document(self, index, data):
        res = self._make_request(
            HTTP_POST,
            path=index+'/_doc',
            data=data,
            headers={ 'Content-Type': 'application/json' }
        )
        return res

    def bulk_upload_document(self, data):
        res = self._make_request(
            HTTP_POST,
            path='/_bulk',
            data=data,
            headers={ 'Content-Type': 'application/json' }
        )
        return res