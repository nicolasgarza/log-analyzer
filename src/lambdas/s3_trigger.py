import json
import boto3
import urllib.parse

s3 = boto3.client("s3")
sqs = boto3.client("sqs")


def lambda_handler(event, context):
    # get object from the event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Record"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    try:
        # get the object
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode("utf-8")

        # basic validation and metadata extraction
        log_count, start_date, end_date = validate_and_extract_metadata(file_content)

        sqs.send_message(
            QueueUrl="YOUR_SQS_QUEUE_URL",
            MessageBody=json.dumps(
                {
                    "bucket": bucket,
                    "key": key,
                    "log_count": log_count,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            ),
        )

        return {
            "statusCode": 200,
            "body": json.dumps("Successfully processed S3 object"),
        }
    except Exception as e:
        print(e)
        return {"statusCode": 500, "body": json.dumps("Error processing s3 object")}


def validate_and_extract_metadata(file_content):
    lines = file_content.split("\n")
    log_count = len(lines)

    # extract dates from first and last valid log entries
    start_date = None
    end_date = None
    for line in lines:
        if line.strip():
            date_str = line.split["["][1].split("]")[0]
            if not start_date:
                start_date = date_str
            end_date = date_str
    return log_count, start_date, end_date
