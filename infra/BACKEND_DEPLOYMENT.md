# Movie Engine Infrastructure

AWS CDK deployment for the Movie Recommendation API.

## Prerequisites

- [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) installed
- AWS credentials configured
- Python 3.8+

## Setup

```bash
cd infra

# Create virtual environment (optional but recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## What Gets Deployed

- **S3 Bucket** (`movie-engine-data`): Stores model files and data
- **Lambda Function** (`movie-recommendation-api`): FastAPI application
  - 3GB memory (for large similarity matrix)
  - 60 second timeout (for S3 cold starts)
  - Python 3.13 runtime
- **HTTP API Gateway**: Public API endpoint
- **IAM Roles**: Lambda execution role with S3 read permissions
- **CloudWatch Logs**: API logs with 1-week retention

## Deployment Steps

### 1. Bootstrap CDK (first time only)

```bash
cdk bootstrap aws://ACCOUNT-NUMBER/REGION
```

### 2. Upload Model Files to S3

Before deploying, upload your model files. The CDK will create the bucket if it doesn't exist, but you need to populate it:

```bash
# Deploy the stack first to create the bucket
cdk deploy

# Then upload model files
aws s3 cp ../movie-engine-data/models/item_similarity_matrix.pkl \
  s3://movie-engine-data/models/item_similarity_matrix.pkl

aws s3 cp ../movie-engine-data/models/user_item_matrix.pkl \
  s3://movie-engine-data/models/user_item_matrix.pkl

aws s3 cp ../movie-engine-data/models/item_based_metadata.json \
  s3://movie-engine-data/models/item_based_metadata.json

aws s3 cp ../movie-engine-data/processed/ml-100k/movies.csv \
  s3://movie-engine-data/processed/ml-100k/movies.csv

aws s3 cp ../movie-engine-data/processed/ml-100k/ratings.csv \
  s3://movie-engine-data/processed/ml-100k/ratings.csv

aws s3 cp ../movie-engine-data/processed/ml-100k/links.csv \
  s3://movie-engine-data/processed/ml-100k/links.csv
```

Or use the sync command:
```bash
aws s3 sync ../movie-engine-data/models s3://movie-engine-data/models
aws s3 sync ../movie-engine-data/processed s3://movie-engine-data/processed
```

### 3. Deploy the Stack

```bash
# See what will be deployed
cdk diff

# Deploy
cdk deploy

# The output will include:
# - API Gateway URL
# - S3 Bucket Name
# - Lambda Function Name
```

### 4. Test the Deployment

```bash
# Get the API URL from outputs or CloudFormation
export API_URL=$(aws cloudformation describe-stacks \
  --stack-name MovieEngineAPIStack \
  --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' \
  --output text)

# Test health check
curl $API_URL/health

# Test recommendations
curl -X POST $API_URL/recommendations \
  -H "Content-Type: application/json" \
  -d '{
    "user_ratings": {"1": 5.0, "50": 4.5},
    "n": 5
  }'
```

## CDK Commands

```bash
# List all stacks
cdk ls

# Show the CloudFormation template
cdk synth

# Compare deployed stack with current state
cdk diff

# Deploy stack
cdk deploy

# Destroy stack (keeps S3 bucket by default)
cdk destroy
```

## Configuration

### Environment Variables

Set in `movie_engine_api_stack.py` Lambda function environment:

- `S3_BUCKET_NAME`: S3 bucket for model files (default: `movie-engine-data`)
- `LOG_LEVEL`: Logging level (default: `INFO`)

### Lambda Configuration

Edit `movie_engine_api_stack.py` to adjust:

- `memory_size`: Lambda memory (default: 3008 MB)
- `timeout`: Function timeout (default: 60 seconds)
- `reserved_concurrent_executions`: Concurrency limit (default: 10)

### API Gateway

The HTTP API includes:
- CORS enabled for all origins
- Lambda proxy integration
- Automatic stage deployment

## Cost Optimization

- **Lambda**: Uses 3GB memory, charged per 100ms of execution
- **API Gateway**: HTTP API is cheaper than REST API
- **S3**: Standard storage for model files (~750MB)
- **CloudWatch**: 1-week log retention

Consider:
- **Provisioned Concurrency**: For consistent performance (adds cost)
- **S3 Intelligent Tiering**: If model files are rarely accessed
- **Lambda Reserved Concurrency**: Limits concurrent executions

## Monitoring

### CloudWatch Logs

```bash
# Stream Lambda logs
aws logs tail /aws/lambda/movie-recommendation-api --follow

# View API Gateway logs
aws logs tail "API-Gateway-Execution-Logs_<api-id>/<stage>" --follow
```

### CloudWatch Metrics

Key metrics to monitor:
- Lambda Duration
- Lambda Errors
- Lambda Invocations
- API Gateway 4XX/5XX errors
- API Gateway Latency

### Alarms (Optional)

Add to `movie_engine_api_stack.py`:
```python
import aws_cdk.aws_cloudwatch as cloudwatch

alarm = cloudwatch.Alarm(
    self, "ErrorAlarm",
    metric=api_function.metric_errors(),
    threshold=10,
    evaluation_periods=1,
)
```

## Troubleshooting

### Deployment fails with bucket already exists
The bucket name `movie-engine-data` must be globally unique. Either:
1. Delete the existing bucket
2. Change the bucket name in `movie_engine_api_stack.py`

### Lambda timeout errors
Increase timeout in `movie_engine_api_stack.py`:
```python
timeout=Duration.seconds(120)
```

### Out of memory errors
Increase Lambda memory:
```python
memory_size=4096  # 4GB
```

### Cold start performance
First request after deployment takes 10-30 seconds. Options:
1. Enable provisioned concurrency (adds cost)
2. Use Lambda warming strategies
3. Implement lazy loading

### S3 access denied
Verify:
- Lambda execution role has S3 read permissions
- Model files exist in bucket: `aws s3 ls s3://movie-engine-data/models/`

## Cleanup

```bash
# Destroy the stack (keeps S3 bucket)
cdk destroy

# Manually delete S3 bucket if needed
aws s3 rm s3://movie-engine-data --recursive
aws s3 rb s3://movie-engine-data
```

## Security

- S3 bucket blocks all public access
- Lambda has minimal IAM permissions (S3 read only)
- API Gateway uses AWS WAF (optional - not included)
- HTTPS only via API Gateway

## Next Steps

- Add custom domain name to API Gateway
- Implement API authentication (Cognito, API keys)
- Add CloudWatch alarms for monitoring
- Set up CI/CD pipeline for automated deployments
- Add AWS WAF for API protection
