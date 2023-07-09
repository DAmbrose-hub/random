import requests
import datetime
import hashlib
import hmac

def get_s3_objects(bucket_name, endpoint, access_key_id, secret_access_key):
    service = 's3'
    region = 'us-east-1'
    method = 'GET'
    path = '/'

    # Get the current timestamp in ISO 8601 format
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

    # Generate the AWS Signature Version 4 authentication headers
    canonical_uri = f'/{bucket_name}'
    canonical_querystring = ''
    canonical_headers = f'host:{endpoint}\nx-amz-date:{timestamp}\n'
    signed_headers = 'host;x-amz-date'
    payload_hash = hashlib.sha256('').hexdigest()
    canonical_request = f'{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}'
    credential_scope = f'{timestamp[:8]}/{region}/{service}/aws4_request'
    string_to_sign = f'AWS4-HMAC-SHA256\n{timestamp}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}'
    signing_key = hmac.new(('AWS4' + secret_access_key).encode(), timestamp[:8].encode(), hashlib.sha256).digest()
    signing_key = hmac.new(signing_key, region.encode(), hashlib.sha256).digest()
    signing_key = hmac.new(signing_key, service.encode(), hashlib.sha256).digest()
    signing_key = hmac.new(signing_key, b'aws4_request', hashlib.sha256).digest()
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    authorization_header = f'AWS4-HMAC-SHA256 Credential={access_key_id}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}'

    # Send the request to S3
    url = f'https://{endpoint}/{bucket_name}'
    headers = {
        'Host': endpoint,
        'X-Amz-Date': timestamp,
        'Authorization': authorization_header
    }
    response = requests.get(url, headers=headers)

    # Parse the response and return the list of objects
    if response.status_code == 200:
        objects = []
        for content in response.content.decode().split('\n'):
            if content != '':
                objects.append(content.split('Key>')[-1].split('</')[0])
        return objects
    else:
        print(f'Error getting S3 objects: {response.text}')
