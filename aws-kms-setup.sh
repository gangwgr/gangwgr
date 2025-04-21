#!/bin/bash
set -euo pipefail

export AWS_REGION="us-east-2"
export KMS_KEY_NAME="rgangwar-openshift-cluster-kek4"

# Extract AWS credentials from the secret
AWS_ACCESS_KEY_ID=$(oc get secret/aws-creds -n kube-system -o json | jq -r '.data.aws_access_key_id' | base64 -d)
AWS_SECRET_ACCESS_KEY=$(oc get secret/aws-creds -n kube-system -o json | jq -r '.data.aws_secret_access_key' | base64 -d)

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY

# Get OpenShift infrastructure name (used as CloudFormation prefix)
OPENSHIFT_INFRA_NAME=$(oc get infrastructure cluster -o json | jq -r '.status.infrastructureName')

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity \
    --region "${AWS_REGION}" \
    --query "Account" \
    --output text)

# Discover the actual IAM master role name from CloudFormation
MASTER_ROLE_NAME=rgangwarkms-22-sg-MasterIamRole-9wR6bbo94EiR
echo "Discovered master IAM role: ${MASTER_ROLE_NAME}"

# Create a new KMS key
KMS_KEY_ID=$(aws kms create-key \
    --region "${AWS_REGION}" \
    --query KeyMetadata.KeyId \
    --output text \
    --description "Used with OpenShift KMS plugin" \
    --key-usage ENCRYPT_DECRYPT)

echo "Created KMS Key: ${KMS_KEY_ID}"

# Check if the alias exists
alias_exists=$(aws kms list-aliases \
    --region "${AWS_REGION}" \
    --query "Aliases[?AliasName=='alias/${KMS_KEY_NAME}'].AliasName" \
    --output text)

if [ "${alias_exists}" == "alias/${KMS_KEY_NAME}" ]; then
    echo "Alias alias/${KMS_KEY_NAME} already exists, skipping creation."
else
    # Create a friendly alias for easier visibility in console
    aws kms create-alias \
        --region "${AWS_REGION}" \
        --alias-name "alias/${KMS_KEY_NAME}" \
        --target-key-id "${KMS_KEY_ID}"

    echo "Created KMS alias: alias/${KMS_KEY_NAME}"
fi

# Render the key policy using discovered values
cat <<EOF > /tmp/policy-rendered.json
{
  "Id": "key-policy-01",
  "Statement": [
    {
      "Sid": "Enable IAM User Permissions",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:root"
      },
      "Action": "kms:*",
      "Resource": "*"
    },
    {
      "Sid": "Allow use of the key",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${MASTER_ROLE_NAME}"
      },
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Allow attachment of persistent resources",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${MASTER_ROLE_NAME}"
      },
      "Action": [
        "kms:CreateGrant",
        "kms:ListGrants",
        "kms:RevokeGrant"
      ],
      "Resource": "*",
      "Condition": {
        "Bool": {
          "kms:GrantIsForAWSResource": "true"
        }
      }
    }
  ]
}
EOF

# Apply the policy to the KMS key
aws kms put-key-policy \
    --region "${AWS_REGION}" \
    --key-id "${KMS_KEY_ID}" \
    --policy-name default \
    --policy file:///tmp/policy-rendered.json

# Output the final key ARN
aws kms describe-key \
    --region "${AWS_REGION}" \
    --key-id "${KMS_KEY_ID}" \
    --query KeyMetadata.Arn \
    --output text

