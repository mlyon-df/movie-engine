"""
CDK Stack for Movie Recommendation Frontend (S3 Only)
Simple S3 bucket for static website hosting
"""

from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    aws_s3 as s3,
    Fn,
)
from constructs import Construct


class MovieEngineFrontendStack(Stack):
    """
    CDK Stack for deploying the Movie Recommendation Frontend
    
    Creates:
    - S3 bucket for static website hosting (public read access)
    
    Note: Frontend must be built with VITE_API_URL set to the API Gateway URL
    before deployment. Get the API URL with:
        aws cloudformation describe-stacks --stack-name MovieEngineAPIStack \
            --query "Stacks[0].Outputs[?ExportName=='MovieEngineApiUrl'].OutputValue" \
            --output text
    """

    def __init__(self, scope: Construct, construct_id: str, api_url: str = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Import API URL from the API stack if not provided
        if api_url is None:
            api_url = Fn.import_value("MovieEngineApiUrl")

        # ========================================
        # S3 Bucket for Frontend
        # ========================================
        frontend_bucket = s3.Bucket(
            self,
            "FrontendBucket",
            bucket_name="movie-engine-frontend",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,  # Clean up when stack is deleted
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=False,
                block_public_policy=False,
                ignore_public_acls=False,
                restrict_public_buckets=False,
            ),
            website_index_document="index.html",
            website_error_document="index.html",  # SPA routing
            public_read_access=True,
        )

        # ========================================
        # Outputs
        # ========================================
        
        CfnOutput(
            self,
            "BucketName",
            value=frontend_bucket.bucket_name,
            description="S3 bucket name for frontend",
            export_name="MovieEngineFrontendBucket",
        )

        CfnOutput(
            self,
            "WebsiteUrl",
            value=frontend_bucket.bucket_website_url,
            description="Website URL",
            export_name="MovieEngineFrontendUrl",
        )
        
        CfnOutput(
            self,
            "ApiUrl",
            value=api_url,
            description="API Gateway URL (imported from API stack)",
        )
