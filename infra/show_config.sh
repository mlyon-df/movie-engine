#!/bin/bash
# Show current stack outputs and configuration

echo "=== Movie Engine Stack Information ==="
echo ""

# API Stack
echo "API Stack:"
echo "---------"
if aws cloudformation describe-stacks --stack-name MovieEngineAPIStack &>/dev/null; then
    API_URL=$(aws cloudformation describe-stacks \
        --stack-name MovieEngineAPIStack \
        --query "Stacks[0].Outputs[?ExportName=='MovieEngineApiUrl'].OutputValue" \
        --output text)
    BUCKET=$(aws cloudformation describe-stacks \
        --stack-name MovieEngineAPIStack \
        --query "Stacks[0].Outputs[?ExportName=='MovieEngineModelBucket'].OutputValue" \
        --output text)
    
    echo "  Status: DEPLOYED"
    echo "  API URL: $API_URL"
    echo "  Model Bucket: $BUCKET"
else
    echo "  Status: NOT DEPLOYED"
fi
echo ""

# Frontend Stack
echo "Frontend Stack:"
echo "---------------"
if aws cloudformation describe-stacks --stack-name MovieEngineFrontendStack &>/dev/null; then
    WEBSITE_URL=$(aws cloudformation describe-stacks \
        --stack-name MovieEngineFrontendStack \
        --query "Stacks[0].Outputs[?ExportName=='MovieEngineFrontendUrl'].OutputValue" \
        --output text)
    
    echo "  Status: DEPLOYED"
    echo "  Website URL: $WEBSITE_URL"
else
    echo "  Status: NOT DEPLOYED"
fi
echo ""

# Check if frontend is configured correctly
if [ -f "../movie-engine-fe/.env" ]; then
    echo "Frontend Configuration:"
    echo "----------------------"
    cat ../movie-engine-fe/.env
    echo ""
else
    echo "Frontend Configuration: NOT SET"
    echo "Run './deploy_frontend.sh' to configure and deploy"
    echo ""
fi
