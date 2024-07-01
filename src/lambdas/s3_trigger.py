import json
import boto3
import urllib.parse
import os

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

sqs_queue_url = os.environ["QUEUE_URL"]


def lambda_handler(event, context):
    # Print the entire event for debugging
    print("Received event:", json.dumps(event))

    # Ensure we have the correct event structure
    if "Records" not in event or len(event["Records"]) == 0:
        print("No records found in the event")
        return {"statusCode": 400, "body": json.dumps("No records found in the event")}

    # get object from the event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"], encoding="utf-8")

    print(f"Processing file: s3://{bucket}/{key}")

    try:
        # get the object
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode("utf-8")

        # Process the file content
        log_count, start_date, end_date = validate_and_extract_metadata(file_content)

        # Send message to SQS
        message_body = json.dumps(
            {
                "bucket": bucket,
                "key": key,
                "log_count": log_count,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        print(f"Sending message to SQS: {message_body}")
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=message_body)

        return {
            "statusCode": 200,
            "body": json.dumps("Successfully processed S3 object"),
        }
    except Exception as e:
        print(f"Error processing S3 object: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing S3 object: {str(e)}"),
        }


def validate_and_extract_metadata(file_content):
    lines = file_content.split("\n")
    log_count = len([line for line in lines if line.strip()])  # Count non-empty lines

    # extract dates from first and last valid log entries
    start_date = None
    end_date = None
    for line in lines:
        if line.strip():
            try:
                date_str = line.split("[")[1].split("]")[0]
                if not start_date:
                    start_date = date_str
                end_date = date_str
            except IndexError:
                print(f"Warning: Unable to parse date from line: {line}")

    return log_count, start_date, end_date
