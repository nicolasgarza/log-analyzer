import json
import boto3
from datetime import datetime, timedelta
import os

cloudwatch = boto3.client("cloudwatch")
sns = boto3.client("sns")
s3 = boto3.client("s3")

sns_topic_arn_link = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        try:
            # Retrieve processed data
            processed_data = get_processed_data(bucket, key)

            # Analyze data
            alerts = analyze_data(processed_data)
            if alerts:
                send_alerts(alerts)

            print(f"Successfully processed file: {bucket}/{key}")
        except Exception as e:
            print(f"Error processing file {bucket}/{key}: {str(e)}")

    return {"statusCode": 200, "body": json.dumps("Monitoring completed successfully")}


def get_processed_data(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def analyze_data(data):
    alerts = []

    # Check for high error rate
    error_rate = (
        data["errors"] / data["total_requests"] if data["total_requests"] > 0 else 0
    )
    if error_rate > 0.05:
        alerts.append(f"High error rate detected: {error_rate:.2%}")

    # Check for traffic spike
    if data["total_requests"] > 10000:  # Adjust this threshold as needed
        alerts.append(
            f"Unusual traffic spike detected: {data['total_requests']} requests"
        )

    return alerts


def send_alerts(alerts):
    message = "\n".join(alerts)
    sns.publish(
        TopicArn=sns_topic_arn_link, Message=message, Subject="Web Server Log Alert"
    )
