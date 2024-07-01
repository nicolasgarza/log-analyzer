import json
import boto3
import re
from datetime import datetime
import os

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")
sqs = boto3.client("sqs")
DLQ_URL = os.environ["DLQ_URL"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]


def send_to_dlq(message, error):
    sqs.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=json.dumps({"original_message": message, "error": str(error)}),
    )


def lambda_handler(event, context):
    for record in event["Records"]:
        message = json.loads(record["body"])
        receipt_handle = record["receiptHandle"]
        bucket = message["bucket"]
        key = message["key"]

        try:
            # get object from s3
            response = s3.get_object(Bucket=bucket, Key=key)
            file_content = response["Body"].read().decode("utf-8")

            # process log file
            processed_data = process_log_file(file_content)

            # store processed data
            store_processed_data(processed_data, key)

            # send metric to cloudwatch
            send_metrics_to_cloudwatch(processed_data)

            # Delete the message from the queue if processing was successful
            delete_message(receipt_handle)

        except Exception as e:
            print(f"Error processing log file: {str(e)}")
            send_to_dlq(json.dumps(message), e)
            # Don't delete the message from the queue, it will be retried

    return {"statusCode": 200, "body": json.dumps("Processing completed")}


def delete_message(receipt_handle):
    try:
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        print(f"Successfully deleted message with receipt handle: {receipt_handle}")
    except Exception as e:
        print(f"Error deleting message: {str(e)}")


def process_log_file(file_content):
    lines = file_content.split("\n")
    processed_data = {
        "total_requests": 0,
        "request_methods": {},
        "status_codes": {},
        "errors": 0,
        "paths": {},
        "ips": {},
    }

    for line in lines:
        if line.strip():
            try:
                log_data = parse_log_line(line)
                processed_data["total_requests"] += 1
                processed_data["request_methods"][log_data["method"]] = (
                    processed_data["request_methods"].get(log_data["method"], 0) + 1
                )
                processed_data["status_codes"][log_data["status"]] = (
                    processed_data["status_codes"].get(log_data["status"], 0) + 1
                )
                processed_data["paths"][log_data["path"]] = (
                    processed_data["paths"].get(log_data["path"], 0) + 1
                )
                processed_data["ips"][log_data["ip"]] = (
                    processed_data["ips"].get(log_data["ip"], 0) + 1
                )
                if log_data["status"].startswith("5"):
                    processed_data["errors"] += 1
            except ValueError as e:
                print(f"Error parsing log line: {str(e)}")

    return processed_data


def store_processed_data(processed_data, key):
    processed_key = f"processed/{key.split('/')[-1]}.json"

    json_data = json.dumps(processed_data)

    s3.put_object(Bucket=PROCESSED_BUCKET, Key=processed_key, Body=json_data)


def send_metrics_to_cloudwatch(processed_data):
    cloudwatch.put_metric_data(
        Namespace="WebServerLogs",
        MetricData=[
            {
                "MetricName": "TotalRequests",
                "Value": processed_data["total_requests"],
                "Unit": "Count",
            },
            {
                "MetricName": "ErrorCount",
                "Value": processed_data["errors"],
                "Unit": "Count",
            },
        ],
    )


def parse_log_line(line):
    # regex to match the log line format
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'
    match = re.match(pattern, line)

    if not match:
        raise ValueError(f"Invalid log line format: {line}")

    ip, timestamp, request, status, bytes_sent, referer, user_agent = match.groups()

    # parse further
    request_parts = request.split()
    method = request_parts[0] if len(request_parts) > 0 else ""
    path = request_parts[1] if len(request_parts) > 1 else ""
    protocol = request_parts[2] if len(request_parts) > 2 else ""

    timestamp = datetime.strptime(timestamp, "%d/%b/%Y:%H:%M:%S %z")

    return {
        "ip": ip,
        "timestamp": timestamp,
        "method": method,
        "path": path,
        "protocol": protocol,
        "status": status,
        "bytes_sent": int(bytes_sent),
        "referer": referer,
        "user_agent": user_agent,
    }
