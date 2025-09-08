#!/bin/bash
# Setup service account for MSAK testing

set -e

PROJECT_ID="ygnahz-eg-codelab"
SERVICE_ACCOUNT_NAME="msak-test-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🔐 Setting up MSAK service account..."

# Create service account
echo "Creating service account: $SERVICE_ACCOUNT_NAME"
# gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
#     --display-name="MSAK Test Service Account" \
#     --description="Service account for testing Managed Service for Apache Kafka" \
#     --project=$PROJECT_ID

# # Grant required IAM roles
# echo "Granting IAM roles..."

# # MSAK client role
# gcloud projects add-iam-policy-binding $PROJECT_ID \
#     --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
#     --role=roles/managedkafka.client

# # Token creator roles (required for OAuth)
# gcloud projects add-iam-policy-binding $PROJECT_ID \
#     --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
#     --role=roles/iam.serviceAccountTokenCreator

# gcloud projects add-iam-policy-binding $PROJECT_ID \
#     --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
#     --role=roles/iam.serviceAccountOpenIdTokenCreator

# Create and download service account key
echo "Creating service account key..."
gcloud iam service-accounts keys create msak-test-sa-key.json \
    --iam-account=$SERVICE_ACCOUNT_EMAIL \
    --project=$PROJECT_ID

echo "✅ Service account setup complete!"
echo ""
echo "📋 Service Account Details:"
echo "   Name: $SERVICE_ACCOUNT_NAME"
echo "   Email: $SERVICE_ACCOUNT_EMAIL"
echo "   Key file: msak-test-sa-key.json"
echo ""
echo "🔧 To use this service account:"
echo "   export GOOGLE_APPLICATION_CREDENTIALS=\$(pwd)/msak-test-sa-key.json"
echo ""
echo "🖥️  To attach to VM:"
echo "   gcloud compute instances set-service-account VM_NAME \\"
echo "     --service-account=$SERVICE_ACCOUNT_EMAIL \\"
echo "     --scopes=https://www.googleapis.com/auth/cloud-platform \\"
echo "     --zone=ZONE"