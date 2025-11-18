"""
CDK Stack for Movie Recommendation Frontend
"""

from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    Duration,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
)
from constructs import Construct


class MovieEngineFrontendStack(Stack):
    """
    CDK Stack for deploying the Movie Recommendation Frontend
    
    Creates:
    - S3 bucket for static website hosting
    - CloudFront distribution for HTTPS and caching
    - S3 deployment for frontend files
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
        # CloudFront Distribution (Optional but Recommended)
        # ========================================
        distribution = cloudfront.Distribution(
            self,
            "Distribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3Origin(frontend_bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
            ),
            default_root_object="index.html",
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.minutes(0),
                ),
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.minutes(0),
                ),
            ],
        )

        # ========================================
        # Deploy Frontend Files
        # ========================================
        s3_deployment.BucketDeployment(
            self,
            "DeployFrontend",
            sources=[s3_deployment.Source.asset("../movie-engine-fe/dist")],
            destination_bucket=frontend_bucket,
            distribution=distribution,
            distribution_paths=["/*"],  # Invalidate CloudFront cache
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
            description="Website URL (S3)",
            export_name="MovieEngineFrontendUrl",
        )

        CfnOutput(
            self,
            "CloudFrontUrl",
            value=f"https://{distribution.distribution_domain_name}",
            description="CloudFront Distribution URL (HTTPS)",
            export_name="MovieEngineCloudFrontUrl",
        )

        CfnOutput(
            self,
            "DistributionId",
            value=distribution.distribution_id,
            description="CloudFront Distribution ID",
            export_name="MovieEngineDistributionId",
        )
