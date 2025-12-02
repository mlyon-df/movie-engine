#!/usr/bin/env python3
"""
AWS CDK App for Movie Recommendation System
Deploys both API and Frontend infrastructure
"""

import aws_cdk as cdk
from movie_engine_api_stack import MovieEngineAPIStack
from movie_engine_frontend_stack_simple import MovieEngineFrontendStack

app = cdk.App()

# API Stack
MovieEngineAPIStack(
    app,
    "MovieEngineAPIStack",
    description="Movie Recommendation API with Lambda, API Gateway, and S3",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-west-2"
    )
)

# Frontend Stack
MovieEngineFrontendStack(
    app,
    "MovieEngineFrontendStack",
    description="Movie Recommendation Frontend with S3 static website hosting",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-west-2"
    )
)

app.synth()
