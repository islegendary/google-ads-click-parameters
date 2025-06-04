import os
import json
from datetime import datetime, timedelta
import boto3
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import yaml

# Environment variables
SECRET_NAME = os.environ['GOOGLE_ADS_SECRET_NAME']
REGION = os.environ.get('AWS_REGION', 'us-east-1')
CUSTOMER_ID = os.environ['GOOGLE_ADS_CUSTOMER_ID']
S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ.get('S3_KEY_PREFIX', 'click_performance/')
DDB_TABLE = os.environ['DYNAMO_TABLE_NAME']
POINTER_KEY = os.environ.get('S3_POINTER_KEY', 'click_performance/last_run_pointer.json')
LOOKBACK_MIN = int(os.environ.get('INCREMENT_MINUTES', '5'))

s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb').Table(DDB_TABLE)


def get_secret():
    sm = boto3.client('secretsmanager', region_name=REGION)
    return json.loads(sm.get_secret_value(SecretId=SECRET_NAME)['SecretString'])


def build_client(creds):
    cfg = {
        'developer_token': creds['developer_token'],
        'client_id': creds['client_id'],
        'client_secret': creds['client_secret'],
        'refresh_token': creds['refresh_token'],
        'login_customer_id': creds['login_customer_id'],
    }
    path = '/tmp/google-ads.yaml'
    with open(path, 'w') as fh:
        fh.write(yaml.dump(cfg))
    return GoogleAdsClient.load_from_storage(path)


def get_last_run():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=POINTER_KEY)
        return json.loads(obj['Body'].read().decode())['last_run_ts']
    except s3.exceptions.NoSuchKey:
        return (datetime.utcnow() - timedelta(minutes=LOOKBACK_MIN)).isoformat()


def set_last_run(ts):
    s3.put_object(Bucket=S3_BUCKET, Key=POINTER_KEY, Body=json.dumps({'last_run_ts': ts}))


def query_clicks(client, customer_id, start_ts, end_ts):
    service = client.get_service('GoogleAdsService')
    query = f"""
        SELECT click_view.gclid, campaign.id, ad_group_ad.ad.id,
               click_view.ad_network_type, segments.date_time
        FROM click_view
        WHERE segments.date_time BETWEEN '{start_ts}' AND '{end_ts}'
    """
    results = []
    try:
        for batch in service.search_stream(customer_id=customer_id, query=query):
            for row in batch.results:
                results.append({
                    'click_performance': {
                        'gclid': row.click_view.gclid,
                        'campaign_id': row.campaign.id,
                        'creative_id': row.ad_group_ad.ad.id,
                        'ad_network_type': row.click_view.ad_network_type.name,
                        'timestamp': row.segments.date_time.value,
                    }
                })
    except GoogleAdsException as exc:
        print('Google Ads API error:', exc)
        raise
    return results


def write_to_dynamodb(items):
    for item in items:
        ddb.put_item(Item={
            'gclid': item['click_performance']['gclid'],
            'campaign_id': item['click_performance']['campaign_id'],
            'creative_id': item['click_performance']['creative_id'],
            'ad_network_type': item['click_performance']['ad_network_type'],
            'timestamp': item['click_performance']['timestamp'],
        })


def lambda_handler(event, context):
    creds = get_secret()
    client = build_client(creds)

    last_run_ts = get_last_run()
    now_ts = datetime.utcnow().isoformat()

    data = query_clicks(client, CUSTOMER_ID, last_run_ts, now_ts)

    if not data:
        set_last_run(now_ts)
        return {'statusCode': 204, 'body': 'no data'}

    ts = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
    key = f"{S3_PREFIX}clicks_{ts}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data))

    write_to_dynamodb(data)

    set_last_run(now_ts)

    return {
        'statusCode': 200,
        'body': f"{len(data)} records written to S3 and DynamoDB",
    }
