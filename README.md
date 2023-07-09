import hashlib
import hmac
import json
import requests
import datetime
from urllib.parse import urlparse

def sign_request_v4(method, url, headers, data, service, secret_key, access_key):
    parsed_url = urlparse(url)
    headers_to_sign = {'host': parsed_url.netloc, 'x-amz-date': headers['X-Amz-Date']}
    canonical_headers = '\n'.join([f"{key}:{headers_to_sign[key]}" for key in sorted(headers_to_sign)])
    signed_headers = ';'.join(sorted(headers_to_sign.keys()))

    # Create canonical request
    canonical_request = '\n'.join([
        method,
        parsed_url.path,
        parsed_url.query,
        canonical_headers + '\n',
        signed_headers,
        hashlib.sha256(data.encode()).hexdigest()
    ])

    # Create string to sign
    date_stamp = headers['X-Amz-Date'][0:8]
    credential_scope = f"{date_stamp}/{service}/s3/aws4_request"
    string_to_sign = '\n'.join([
        'AWS4-HMAC-SHA256',
        headers['X-Amz-Date'],
        credential_scope,
        hashlib.sha256(canonical_request.encode()).hexdigest()
    ])

    # Calculate signature
    signing_key = get_signature_key(secret_key, date_stamp, service, 'aws4_request')
    signature = hmac.new(signing_key, (string_to_sign).encode(), hashlib.sha256).hexdigest()

    # Add authorization header
    authorization_header = f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    headers['Authorization'] = authorization_header

    return headers

def get_signature_key(key, date_stamp, service_name, region_name):
    k_date = hmac.new(('AWS4' + key).encode('utf-8'), date_stamp.encode('utf-8'), hashlib.sha256).digest()
    k_region = hmac.new(k_date, service_name.encode('utf-8'), hashlib.sha256).digest()
    k_service = hmac.new(k_region, region_name.encode('utf-8'), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b'aws4_request', hashlib.sha256).digest()
    return k_signing

def list_objects(bucket_name, endpoint, access_key, secret_key):
    method = 'GET'
    now = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    url = f"{endpoint}/{bucket_name}"
    headers = {
        'X-Amz-Date': now
    }
    signed_headers = sign_request_v4(method, url, headers, '', 's3', secret_key, access_key)
    headers.update(signed_headers)
    response = requests.get(url, headers=headers)
    return json.loads(response.text)

bucket_name = 'my-bucket'
endpoint = 'https://s3.amazonaws.com'
access_key = 'MY_ACCESS_KEY'
secret_key = 'MY_SECRET_KEY'

