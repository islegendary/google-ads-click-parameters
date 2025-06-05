# Google Ads Click Parameters

This repository contains a sample AWS Lambda function for collecting click
performance data from the Google Ads API and storing it in S3 and DynamoDB.
It also provides a small utility script for exporting an initial historical
load from a warehouse table.

## Lambda Function

`lambda_function.py` downloads recent clicks from Google Ads. Each invocation
queries all clicks since the last successful run (tracked in an RDS table) and
writes them to both DynamoDB and S3.

Set the following environment variables in your Lambda configuration:

- `GOOGLE_ADS_SECRET_NAME` – name of the Secrets Manager secret with OAuth
  and Ads API credentials.
- `S3_BUCKET` – S3 bucket for JSON dumps.
- `DYNAMO_TABLE_NAME` – DynamoDB table for lookup by gclid.
- `RDS_HOST`, `RDS_DB`, `RDS_USER`, `RDS_PASSWORD` and optional `RDS_PORT`
  configure the Postgres instance used for timestamp tracking.
- Optional variables such as `S3_KEY_PREFIX` and `INCREMENT_MINUTES` control
  S3 locations and the query lookback window.

The Lambda automatically refreshes the OAuth access token using the stored
refresh token on each invocation. If Google returns a new refresh token it is
written back to Secrets Manager so subsequent runs use the updated credential.

## Initial Load

`initial_load.py` provides a minimal example of exporting historical data from a
warehouse (using the Snowflake Python connector) and loading it into the same
S3 bucket and DynamoDB table. Update the Snowflake connection environment
variables before running it locally or from a one‑off container.

## Running a Syntax Check

To verify the code parses correctly, run:

```bash
python -m py_compile lambda_function.py initial_load.py
```
