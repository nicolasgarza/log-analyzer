import json
import boto3
from datetime import datetime, timedelta

cloudwatch = boto3.client("cloudwatch")
sns = boto3.client("sns")


def lambda_handler(event, context):
    # assume teh event contains teh s3 bucket and key of the processed data
    bucket = event["bucket"]
    key = event["key"]

    try:
        # retrieve processed data
        processed_data = get_processed_data(bucket, key)

        # analyze data
        alerts = analyze_data(processed_data)
        if alerts:
            send_alerts(alerts)

        return {
            "statusCode": 200,
            "body": json.dumps("Monitoring completed successfully"),
        }
    except Exception as e:
        print(f"Error in monitoring: {str(e)}")
        return {"statusCode": 500, "body": json.dumps("Error in monitoring function")}


def get_processed_data(bucket, key):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def analyze_data(data):
    alerts = []

    # check for high error rate
    error_rate = (
        data["errors"] / data["total_requests"] if data["total_requests"] > 0 else 0
    )
    if error_rate > 0.05:
        alerts.append(f"High error rate detected: {error_rate:.2%}")

    # Check for traffic spike
    # should probably check with historical data
    if data["total_requests"] > 10000:
        alerts.append(
            f"Unusual traffic spike detected: {data['total_requests']} requests"
        )

    return alerts


def send_alerts(alerts):
    sns_topic_arn = "SNS_TOPIC_ARN"  # replace
    message = "\n".join(alerts)
    sns.publish(TopicArn=sns_topic_arn, Message=message, Subject="Web Server Log Alert")
