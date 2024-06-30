from src.data_ingestion import ingest_data


def main():
    region_name = "your-region"
    stream_name = "your-firehose-stream-name"
    data_file_path = "data/logs"
    ingest_data(region_name, stream_name, data_file_path)


if __name__ == "__main__":
    main()
