#!/usr/bin/env python3
"""
AWS CDK App for Movie Recommendation API
Deploys Lambda function with API Gateway and S3 bucket for model files
"""

import aws_cdk as cdk
from movie_engine_api_stack import MovieEngineAPIStack

app = cdk.App()

MovieEngineAPIStack(
    app,
    "MovieEngineAPIStack",
    description="Movie Recommendation API with Lambda, API Gateway, and S3",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1"
    )
)

app.synth()
