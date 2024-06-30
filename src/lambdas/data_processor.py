import json
import boto3
from utils.data_utils import parse_log_line

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")


def lambda_handler(event, context):
    # extract message from sqs event
    message = json.loads(event["Records"][0]["body"])
    bucket = message["bucket"]
    key = message["key"]

    try:
        # get object from s3
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode("utf-8")

        # process log file
        processed_data = process_log_file(file_content)

        # store processed data
        store_processed_data(processed_data, bucket, key)

        # send metric to cloudwatch
        send_metrics_to_cloudwatch(processed_data)

        return {
            "statusCode": 200,
            "body": json.dumps("Successfully processed log file"),
        }
    except Exception as e:
        print(f"Error processing log file: {str(e)}")
        return {"statusCode": 500, "body": json.dumps("Error processing log file")}


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


def store_processed_data(processed_data, bucket, key):
    processed_key = f"processed/{key.split('/')[-1]}.json"

    json_data = json.dumps(processed_data)

    s3.put_object(Bucket=bucket, key=processed_key, Body=json_data)


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
