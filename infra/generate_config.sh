#!/bin/bash
# Generate runtime config for frontend
# This creates a config.js file that can be loaded at runtime

set -e

echo "Generating runtime configuration..."

# Get API URL from CloudFormation stack
API_URL=$(aws cloudformation describe-stacks \
    --stack-name MovieEngineAPIStack \
    --query "Stacks[0].Outputs[?ExportName=='MovieEngineApiUrl'].OutputValue" \
    --output text 2>/dev/null)

if [ -z "$API_URL" ]; then
    echo "Error: Could not fetch API URL from CloudFormation stack"
    echo "Make sure MovieEngineAPIStack is deployed"
    exit 1
fi

# Create config.js file
cat > ../movie-engine-fe/public/config.js << EOF
// Auto-generated configuration
window.APP_CONFIG = {
  API_URL: "${API_URL}"
};
EOF

echo "Configuration generated:"
echo "  API_URL: $API_URL"
echo ""
echo "Config file: movie-engine-fe/public/config.js"
