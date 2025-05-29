from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3_notifications as s3n,
)
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from constructs import Construct
import os

class RearcQuestStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. S3 Bucket
        bucket = s3.Bucket.from_bucket_name(self, "RearcQuestV2Bucket", "rearcquestv2")

        # 2. SQS Queue
        queue = sqs.Queue(self, "DataPipelineQueue",
            visibility_timeout=Duration.seconds(60))

        # 3. Lambda Function 1 - SyncBLSandAPI
        sync_lambda = _lambda.Function(self, "SyncBLSandAPIFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="sync_bls_api.lambda_handler",
            code=_lambda.Code.from_asset("rearc_quest/lambda"),
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": bucket.bucket_name
            }
        )
        bucket.grant_write(sync_lambda)

        # 4. CloudWatch Event Rule (Daily Schedule)
        rule = events.Rule(self, "DailySyncRule",
            schedule=events.Schedule.rate(Duration.days(1))
        )
        rule.add_target(targets.LambdaFunction(sync_lambda))

        # 5. Lambda Function 2 - AnalyticsProcessor
        analytics_lambda = _lambda.Function(self, "AnalyticsProcessorFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="analysis.lambda_handler",
            code=_lambda.Code.from_asset("rearc_quest/lambda"),
            timeout=Duration.seconds(60),
            environment={
                "BUCKET_NAME": bucket.bucket_name
            }
        )
        bucket.grant_read(analytics_lambda)

        # Grant permission to receive SQS messages
        queue.grant_consume_messages(analytics_lambda)

        # Set SQS as trigger for analytics Lambda
        analytics_lambda.add_event_source(SqsEventSource(queue, batch_size=1))

        # 6. Add S3 event notification for JSON file upload
        notification = s3n.SqsDestination(queue)
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED_PUT, 
            notification, 
            s3.NotificationKeyFilter(prefix="population-data/", suffix=".json"))

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "RearcQuestQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )
