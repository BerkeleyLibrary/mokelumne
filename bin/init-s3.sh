#!/bin/bash

if [ -n "$BUCKET_NAME" ]; then
    echo "Creating bucket: s3://$BUCKET_NAME"
    awslocal s3 mb s3://"$BUCKET_NAME"

    if [ -n "$AWS_SEED_DATA" ]; then
        echo "AWS_SEED_DATA is set ($AWS_SEED_DATA). Syncing files..."
        awslocal s3 sync /tmp/aws_seed_data/ s3://"$BUCKET_NAME"/
    else
        echo "AWS_SEED_DATA is not set. Skipping file seeding."
    fi
else
    echo "No BUCKET_NAME environment variable specified. Skipping initialization."
fi
