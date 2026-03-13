import os

from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration, #used for timeout
    CfnOutput,
    RemovalPolicy,
)
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_secretsmanager as secretsmanager


class StocksPipeline1Stack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table = dynamodb.Table(
            self, "StocksTable",
            partition_key=dynamodb.Attribute(name="date", type=dynamodb.AttributeType.STRING),
            #sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING),
            removal_policy=cdk.RemovalPolicy.DESTROY,            # dev only
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )
        secret_name = "stocks/massive-api-key"
        secret = secretsmanager.Secret.from_secret_name_v2(self, "MassiveApiKeySecret", secret_name)

       

        #lambda (eventbridge->lambda->dynamodb)
        #responsibility is to fetch stock data, find top movers, write to DB
        api_fetch = _lambda.Function(
            self, "ApiFetchFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="api_fetch.handler",   # file: lambda/api_fetch.py, function: handler
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(60),
            environment={
                "TABLE_NAME": table.table_name,
                "MASSIVE_API_KEY_SECRET_ARN": secret.secret_arn,
            }
        )
        # grant write permission to the Lambda for the table
        table.grant_write_data(api_fetch)
        secret.grant_read(api_fetch)

        #cron job
        rule = events.Rule(
            self, "DailyRule",
            schedule=events.Schedule.cron(minute="0", hour="23", week_day="MON-FRI")  # UTC midnight
        )
        rule.add_target(targets.LambdaFunction(api_fetch, retry_attempts=2,))

        #retrieval lambda (API Gateway -> Lambda -> DynamoDB)
        #returns last 7 days of winner as JSON
        api_retrieve = _lambda.Function(
            self, "ApiRetrieveFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="retrieval.handler", 
            code=_lambda.Code.from_asset("lambda"),  
            timeout=Duration.seconds(60),
            memory_size=128,
            environment={
                "TABLE_NAME": table.table_name,
            }
        )
        table.grant_read_data(api_retrieve)

        #api gateway -> retrieval lambda
        api = apigateway.RestApi(self, "StocksApi",
            rest_api_name="Stocks Service",
            description="Stocks top mover REST API",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=["GET", "OPTIONS"],
                allow_headers=["Content-Type"],
            ),
            deploy_options=apigateway.StageOptions(stage_name="prod", throttling_rate_limit=100, throttling_burst_limit=200),
        )

        movers = api.root.add_resource("movers")
        movers.add_method("GET", apigateway.LambdaIntegration(api_retrieve),)

        # outputs
        CfnOutput(self, "TableName", value=table.table_name, description="DynamoDB table name")
        CfnOutput(self, "ApiUrl", value=f"{api.url}movers", description="Base URL for the REST API")
        CfnOutput(self, "IngestFunctionName", value=api_fetch.function_name, description="Ingestionlambda (triggered by eventbridge)")
        CfnOutput(self, "RetrievalFunctionName", value=api_retrieve.function_name, description="Retrieval lambda (triggered by API Gateway)")