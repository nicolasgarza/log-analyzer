from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_lambda_event_sources as lambda_events,
    aws_iam as iam,
    Duration,
)
from constructs import Construct


class LogProcessingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # create s3 buckets
        raw_logs_bucket = s3.Bucket(self, "RawLogsBucket")
        processed_logs_bucket = s3.Bucket(self, "ProcessedLogsBucket")

        # Create SQS queue
        processing_queue = sqs.Queue(self, "ProcessingQueue")

        # Create SNS topic for alerts
        alert_topic = sns.Topic(self, "AlertTopic")

        # Create Lambda Functions
        s3_trigger_lambda = lambda_.Function(
            self,
            "S3TriggerLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="s3_trigger.lambda_handler",
            code=lambda_.Code.from_asset("src/lambdas"),
            environment={"QUEUE_URL": processing_queue.queue_url},
        )

        data_processor_lambda = lambda_.Function(
            self,
            "DataProcessorLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="data_processor.lambda_handler",
            code=lambda_.Code.from_asset("src/lambdas"),
            environment={"PROCESSED_BUCKET": processed_logs_bucket.bucket_name},
        )

        monitor_lambda = lambda_.Function(
            self,
            "MonitorLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="monitor.lambda_handler",
            code=lambda_.Code.from_asset("src/lambdas"),
            environment={"SNS_TOPIC_ARN": alert_topic.topic_arn},
        )

        # set up s3 event trigger
        s3_trigger_lambda.add_event_source(
            lambda_events.S3EventSource(
                raw_logs_bucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="logs/", suffix=".log")],
            )
        )

        # set up sqs trigger for data proecssor
        data_processor_lambda.add_event_source(
            lambda_events.SqsEventSource(processing_queue)
        )

        # grant permissions
        raw_logs_bucket.grant_read(s3_trigger_lambda)
        processing_queue.grant_send_messages(s3_trigger_lambda)

        raw_logs_bucket.grant_read(data_processor_lambda)
        processed_logs_bucket.grant_write(data_processor_lambda)

        processed_logs_bucket.grant_read(monitor_lambda)
        alert_topic.grant_publish(monitor_lambda)

        # add cloudwatch permissions to all lambdas
        for func in [s3_trigger_lambda, data_processor_lambda, monitor_lambda]:
            func.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"], resources=["*"]
                )
            )
