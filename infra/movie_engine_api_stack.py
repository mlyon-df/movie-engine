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
    aws_events as events,
    aws_events_targets as targets,
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
    - Optional scheduled warming or provisioned concurrency to reduce cold starts
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get TMDB API key from environment
        tmdb_api_key = os.environ.get("TMDB_API_KEY", "")
        if not tmdb_api_key:
            print("WARNING: TMDB_API_KEY not set. Movie metadata features will not work.")

        # Get provisioned concurrency configuration
        enable_provisioned = os.environ.get("ENABLE_PROVISIONED_CONCURRENCY", "false").lower() == "true"
        provisioned_instances = int(os.environ.get("PROVISIONED_CONCURRENCY_COUNT", "1"))

        # Get warming configuration (default: enabled if provisioned concurrency is disabled)
        enable_warming = os.environ.get("ENABLE_WARMING", "true" if not enable_provisioned else "false").lower() == "true"
        warming_rate_minutes = int(os.environ.get("WARMING_RATE_MINUTES", "5"))

        # ========================================
        # S3 Bucket for Model Files
        # ========================================
        # CDK will create the bucket if it doesn't exist
        # If it already exists and you own it, CloudFormation will adopt it
        # The bucket is retained when the stack is deleted (RemovalPolicy.RETAIN)
        model_bucket = s3.Bucket(
            self,
            "ModelBucket",
            bucket_name="movie-engine-data",
            versioned=False,
            removal_policy=RemovalPolicy.RETAIN,  # Keep bucket when stack is deleted
            auto_delete_objects=False,  # Don't delete objects on stack deletion
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # ========================================
        # Lambda Function
        # ========================================
        
        # Lambda layer for dependencies (pandas, numpy are large)
        # This keeps the main function package small
        # The layer only bundles requirements.txt dependencies, not application code
        dependencies_layer = lambda_.LayerVersion(
            self,
            "DependenciesLayer",
            code=lambda_.Code.from_asset(
                "../movie-engine-api",
                bundling={
                    "image": lambda_.Runtime.PYTHON_3_13.bundling_image,
                    "command": [
                        "bash",
                        "-c",
                        # Only install dependencies from requirements.txt, nothing else
                        "pip install -r requirements.txt -t /asset-output/python --no-cache-dir && "
                        "rm -rf /asset-output/python/*.dist-info && "
                        "find /asset-output/python -type d -name '__pycache__' -exec rm -rf {} + || true && "
                        "find /asset-output/python -type f -name '*.pyc' -delete"
                    ],
                },
            ),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            description="Dependencies for Movie Recommendation API (pandas, numpy, fastapi)",
        )

        # Build environment variables for Lambda
        lambda_env = {
            "S3_BUCKET_NAME": model_bucket.bucket_name,
            "LOG_LEVEL": "INFO",
        }
        
        # Add TMDB API key if provided
        if tmdb_api_key:
            lambda_env["TMDB_API_KEY"] = tmdb_api_key

        # Main Lambda function
        api_function = lambda_.Function(
            self,
            "ApiFunction",
            function_name="movie-recommendation-api",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="main.handler",
            code=lambda_.Code.from_asset(
                "../movie-engine-api",
                exclude=[
                    ".venv",
                    ".venv/**",
                    "__pycache__",
                    "__pycache__/**",
                    "**/__pycache__",
                    "**/__pycache__/**",
                    "*.pyc",
                    "**/*.pyc",
                    ".pytest_cache",
                    ".pytest_cache/**",
                    "tests",
                    "tests/**",
                    "test_*.py",
                    "**/test_*.py",
                    "run_local.py",
                    ".vscode",
                    ".vscode/**",
                    "requirements.txt",
                    ".git",
                    ".git/**",
                    "*.md",
                    "**/*.md",
                ]
            ),
            layers=[dependencies_layer],
            memory_size=3008,  # 3GB - needed for large similarity matrix
            timeout=Duration.seconds(60),  # Allow time for cold start S3 downloads
            environment=lambda_env,
            log_retention=logs.RetentionDays.ONE_WEEK
        )

        # Grant S3 read permissions to Lambda
        model_bucket.grant_read(api_function)

        # ========================================
        # Provisioned Concurrency (Optional)
        # ========================================
        if enable_provisioned:
            print(f"Enabling provisioned concurrency with {provisioned_instances} instance(s)")
            print(f"Note: This will incur additional costs (~${10 * provisioned_instances}-${20 * provisioned_instances}/month)")
            
            # Create a version for the function
            version = api_function.current_version
            
            # Create an alias pointing to the version with provisioned concurrency
            alias = lambda_.Alias(
                self,
                "ApiLiveAlias",
                alias_name="live",
                version=version,
                provisioned_concurrent_executions=provisioned_instances,
            )
            
            # Use the alias for API Gateway integration
            lambda_integration_target = alias
            print("Provisioned concurrency enabled. Lambda will stay warm.")
        else:
            print("Provisioned concurrency disabled. Set ENABLE_PROVISIONED_CONCURRENCY=true to enable.")
            lambda_integration_target = api_function

        # ========================================
        # Scheduled Warming (Optional)
        # ========================================
        if enable_warming and not enable_provisioned:
            print(f"Enabling scheduled warming every {warming_rate_minutes} minutes")
            print("Note: This keeps 1 container warm but doesn't guarantee no cold starts")
            
            # Create EventBridge rule to trigger Lambda every N minutes
            warming_rule = events.Rule(
                self,
                "WarmingRule",
                schedule=events.Schedule.rate(Duration.minutes(warming_rate_minutes)),
                description=f"Pings Lambda every {warming_rate_minutes} minutes to keep it warm",
            )
            
            # Add Lambda as target
            warming_rule.add_target(
                targets.LambdaFunction(
                    lambda_integration_target,
                    event=events.RuleTargetInput.from_object({
                        "warmer": True,
                        "path": "/health"
                    })
                )
            )
        elif enable_provisioned:
            print("Scheduled warming disabled (provisioned concurrency is enabled).")
        else:
            print("Scheduled warming disabled. Set ENABLE_WARMING=true to enable.")

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

        # Lambda integration (uses alias if provisioned concurrency is enabled)
        lambda_integration = integrations.HttpLambdaIntegration(
            "LambdaIntegration",
            lambda_integration_target,
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
        
        if enable_provisioned:
            CfnOutput(
                self,
                "AliasName",
                value=alias.alias_name,
                description="Lambda alias with provisioned concurrency",
            )
            CfnOutput(
                self,
                "ProvisionedInstances",
                value=str(provisioned_instances),
                description="Number of provisioned concurrent instances",
            )
        
        if enable_warming:
            CfnOutput(
                self,
                "WarmingSchedule",
                value=f"Every {warming_rate_minutes} minutes",
                description="Lambda warming schedule",
            )