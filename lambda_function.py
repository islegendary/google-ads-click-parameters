import os
import json
import logging
from datetime import datetime, timedelta
import psycopg2
import boto3
import yaml

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# --- Environment ---
SECRET_NAME = os.environ['GOOGLE_ADS_SECRET_NAME']
REGION = os.environ.get('AWS_REGION', 'us-east-1')
RDS_HOST = os.environ['RDS_HOST']
RDS_DB = os.environ['RDS_DB']
RDS_USER = os.environ['RDS_USER']
RDS_PASSWORD = os.environ['RDS_PASSWORD']
RDS_PORT = int(os.environ.get('RDS_PORT', '5432'))
S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ.get('S3_KEY_PREFIX', 'click_performance/')
DDB_TABLE = os.environ['DYNAMO_TABLE_NAME']
LOOKBACK_MIN = int(os.environ.get('INCREMENT_MINUTES', '5'))

# --- AWS Clients ---
sm = boto3.client('secretsmanager', region_name=REGION)
s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb').Table(DDB_TABLE)

# --- Core Functions ---
def get_secret():
    return json.loads(sm.get_secret_value(SecretId=SECRET_NAME)['SecretString'])

def build_client(creds):
    path = '/tmp/google-ads.yaml'
    yaml_config = {
        'developer_token': creds['developer_token'],
        'client_id': creds['client_id'],
        'client_secret': creds['client_secret'],
        'refresh_token': creds['refresh_token'],
        'login_customer_id': creds['login_customer_id'],
    }
    with open(path, 'w') as fh:
        yaml.dump(yaml_config, fh)
    return GoogleAdsClient.load_from_storage(path)

def get_last_run():
    try:
        conn = psycopg2.connect(
            host=RDS_HOST, database=RDS_DB,
            user=RDS_USER, password=RDS_PASSWORD, port=RDS_PORT
        )
        with conn.cursor() as cur:
            cur.execute("SELECT last_timestamp FROM gclid_tracking ORDER BY last_timestamp DESC LIMIT 1")
            row = cur.fetchone()
            if not row or not row[0]:
                return (datetime.utcnow() - timedelta(minutes=LOOKBACK_MIN)).isoformat()
            return row[0].isoformat()
    except Exception as e:
        logger.error(f"Error reading last timestamp from RDS: {e}")
        raise
    finally:
        if conn:
            conn.close()

def set_last_run(ts):
    try:
        conn = psycopg2.connect(
            host=RDS_HOST, database=RDS_DB,
            user=RDS_USER, password=RDS_PASSWORD, port=RDS_PORT
        )
        with conn.cursor() as cur:
            cur.execute("UPDATE gclid_tracking SET last_timestamp = %s", (ts,))
            conn.commit()
    except Exception as e:
        logger.error(f"Error updating last timestamp in RDS: {e}")
        raise
    finally:
        if conn:
            conn.close()

def list_customer_ids(client):
    service = client.get_service("CustomerService")
    response = service.list_accessible_customers()
    return [res.replace("customers/", "") for res in response.resource_names]

def query_clicks(client, customer_id, start_ts, end_ts):
    service = client.get_service("GoogleAdsService")
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
                    'gclid': row.click_view.gclid,
                    'campaign_id': row.campaign.id,
                    'creative_id': row.ad_group_ad.ad.id,
                    'ad_network_type': row.click_view.ad_network_type.name,
                    'timestamp': row.segments.date_time.value,
                    'customer_id': customer_id,
                })
    except GoogleAdsException as exc:
        logger.warning(f"Google Ads error for customer {customer_id}: {exc}")
    return results

def write_to_dynamodb(items):
    for item in items:
        ddb.put_item(Item=item)

# --- Lambda Entrypoint ---
def lambda_handler(event, context):
    creds = get_secret()
    client = build_client(creds)

    start_ts = get_last_run()
    end_ts = datetime.utcnow().isoformat()

    logger.info(f"Running for time window: {start_ts} -> {end_ts}")
    all_data = []

    for cid in list_customer_ids(client):
        logger.info(f"Processing customer {cid}")
        rows = query_clicks(client, cid, start_ts, end_ts)
        all_data.extend(rows)

    if not all_data:
        set_last_run(end_ts)
        return {'statusCode': 204, 'body': 'No data'}

    ts = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
    key = f"{S3_PREFIX}clicks_{ts}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(all_data))

    write_to_dynamodb(all_data)
    set_last_run(end_ts)

    return {
        'statusCode': 200,
        'body': f"Wrote {len(all_data)} records from {start_ts} to {end_ts}"
    }
