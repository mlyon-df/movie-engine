"""
CDK Stack for Movie Recommendation API
"""

from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    RemovalPolicy,
    aws_lambda as lambda_,
    aws_apigatewayv2 as apigw,
    aws_apigatewayv2_integrations as integrations,
    aws_s3 as s3,
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct
import os


class MovieEngineAPIStack(Stack):
    """
    CDK Stack for deploying the Movie Recommendation API
    
    Creates:
    - S3 bucket for model files
    - Lambda function for API
    - HTTP API Gateway
    - IAM roles and permissions
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ========================================
        # S3 Bucket for Model Files
        # ========================================
        model_bucket = s3.Bucket(
            self,
            "ModelBucket",
            bucket_name="movie-engine-data",
            versioned=False,
            removal_policy=RemovalPolicy.RETAIN,  # Keep data when stack is deleted
            auto_delete_objects=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # ========================================
        # Lambda Function
        # ========================================
        
        # Build the Lambda layer for dependencies (pandas, numpy are large)
        # This keeps the main function package small
        dependencies_layer = lambda_.LayerVersion(
            self,
            "DependenciesLayer",
            code=lambda_.Code.from_asset(
                "../movie-engine-api",
                bundling={
                    "image": lambda_.Runtime.PYTHON_3_11.bundling_image,
                    "command": [
                        "bash",
                        "-c",
                        "pip install -r requirements.txt -t /asset-output/python && "
                        "rm -rf /asset-output/python/*.dist-info"
                    ],
                },
            ),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_11],
            description="Dependencies for Movie Recommendation API (pandas, numpy, fastapi)",
        )

        # Main Lambda function
        api_function = lambda_.Function(
            self,
            "ApiFunction",
            function_name="movie-recommendation-api",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=lambda_.Code.from_asset(
                "../movie-engine-api",
                exclude=[
                    "venv",
                    "__pycache__",
                    "*.pyc",
                    ".pytest_cache",
                    "tests",
                    "test_*.py",
                    "run_local.py",
                    ".vscode",
                ]
            ),
            layers=[dependencies_layer],
            memory_size=3008,  # 3GB - needed for large similarity matrix
            timeout=Duration.seconds(60),  # Allow time for cold start S3 downloads
            environment={
                "S3_BUCKET_NAME": model_bucket.bucket_name,
                "LOG_LEVEL": "INFO",
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
            reserved_concurrent_executions=10,  # Limit concurrent executions
        )

        # Grant S3 read permissions to Lambda
        model_bucket.grant_read(api_function)

        # ========================================
        # API Gateway
        # ========================================
        
        # HTTP API (cheaper and simpler than REST API)
        http_api = apigw.HttpApi(
            self,
            "HttpApi",
            api_name="movie-recommendation-api",
            description="Movie Recommendation API using Item-Based Collaborative Filtering",
            cors_preflight={
                "allow_origins": ["*"],
                "allow_methods": [
                    apigw.CorsHttpMethod.GET,
                    apigw.CorsHttpMethod.POST,
                    apigw.CorsHttpMethod.OPTIONS,
                ],
                "allow_headers": ["*"],
            },
        )

        # Lambda integration
        lambda_integration = integrations.HttpLambdaIntegration(
            "LambdaIntegration",
            api_function,
        )

        # Add routes
        http_api.add_routes(
            path="/{proxy+}",
            methods=[apigw.HttpMethod.ANY],
            integration=lambda_integration,
        )

        # ========================================
        # Outputs
        # ========================================
        
        CfnOutput(
            self,
            "ApiUrl",
            value=http_api.url or "https://not-deployed-yet",
            description="API Gateway URL",
            export_name="MovieEngineApiUrl",
        )

        CfnOutput(
            self,
            "BucketName",
            value=model_bucket.bucket_name,
            description="S3 bucket name for model files",
            export_name="MovieEngineModelBucket",
        )

        CfnOutput(
            self,
            "FunctionName",
            value=api_function.function_name,
            description="Lambda function name",
            export_name="MovieEngineFunctionName",
        )

        CfnOutput(
            self,
            "FunctionArn",
            value=api_function.function_arn,
            description="Lambda function ARN",
            export_name="MovieEngineFunctionArn",
        )
