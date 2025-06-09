// Utility to export historical Google Ads click data from Snowflake using Node.js
// Requires the snowflake-sdk and aws-sdk packages.

const snowflake = require('snowflake-sdk');
const AWS = require('aws-sdk');

// --- Environment variables for Snowflake ---
const {
  SNOWFLAKE_ACCOUNT,
  SNOWFLAKE_USER,
  SNOWFLAKE_PASSWORD,
  SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_DATABASE,
  SNOWFLAKE_SCHEMA,
  S3_BUCKET,
  S3_KEY_PREFIX = 'click_performance/',
  DYNAMO_TABLE_NAME,
} = process.env;

const TABLE_NAME =
  'SEGMENT_EVENTS.GOOGLE_ADS_CLICK_PARAMETERS.CLICK_PERFORMANCE_REPORTS';

// --- AWS Clients ---
const s3 = new AWS.S3();
const ddb = new AWS.DynamoDB.DocumentClient();

function fetchAllRows() {
  return new Promise((resolve, reject) => {
    const connection = snowflake.createConnection({
      account: SNOWFLAKE_ACCOUNT,
      username: SNOWFLAKE_USER,
      password: SNOWFLAKE_PASSWORD,
      warehouse: SNOWFLAKE_WAREHOUSE,
      database: SNOWFLAKE_DATABASE,
      schema: SNOWFLAKE_SCHEMA,
    });

    connection.connect(err => {
      if (err) {
        reject(err);
        return;
      }
      connection.execute({
        sqlText: `SELECT * FROM ${TABLE_NAME}`,
        complete: (err, stmt, rows) => {
          connection.destroy();
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        },
      });
    });
  });
}

async function writeToDynamoDB(rows) {
  const batches = [];
  let current = [];
  for (const row of rows) {
    current.push({ PutRequest: { Item: row } });
    if (current.length === 25) {
      batches.push(current);
      current = [];
    }
  }
  if (current.length) batches.push(current);

  for (const batch of batches) {
    const params = {
      RequestItems: {
        [DYNAMO_TABLE_NAME]: batch,
      },
    };
    await ddb.batchWrite(params).promise();
  }
}

async function dumpToS3(rows, key) {
  const params = {
    Bucket: S3_BUCKET,
    Key: key,
    Body: JSON.stringify(rows),
  };
  await s3.putObject(params).promise();
}

async function main() {
  try {
    const rows = await fetchAllRows();
    const key = `${S3_KEY_PREFIX}initial_load.json`;
    await dumpToS3(rows, key);
    await writeToDynamoDB(rows);
    console.log(`Wrote ${rows.length} records to S3 and DynamoDB`);
  } catch (err) {
    console.error(err);
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main();
}
