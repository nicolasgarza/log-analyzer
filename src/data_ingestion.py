import json
import boto3
import time

# Code for sending data to kinesis firehose


def get_firehose_client(region_name):
    return boto3.client("firehose", region_name=region_name)


def send_to_firehose(firehose_client, data, stream_name):
    response = firehose_client.put_record(
        DeliveryStreamName=stream_name, Record={"Data": json.dumps(data) + "\n"}
    )
    return response


def ingest_data(region_name, stream_name, data_file_path):
    firehose_client = get_firehose_client(region_name)

    with open(data_file_path, "r") as f:
        log_data = json.load(f)

    for log_entry in log_data:
        response = send_to_firehose(firehose_client, log_entry, stream_name)
        print(f"Sent log entry. Response: {response}")
        time.sleep(0.05)


if __name__ == "__main__":
    region_name = "your-region"
    stream_name = "your-firehose-stream-name"
    data_file_path = "../data/logs.json"
    ingest_data(region_name, stream_name, data_file_path)
