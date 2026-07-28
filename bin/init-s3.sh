#!/bin/bash

echo "=================================================="
echo "    STARTING S3 SEEDING PROCESS           "
echo "=================================================="

if [ -n "$AWS_CITY_ARTS_BUCKET" ]; then
    echo "Creating City Arts bucket: s3://$AWS_CITY_ARTS_BUCKET"
    awslocal s3 mb s3://"$AWS_CITY_ARTS_BUCKET"

    if [ -d "/tmp/aws_seed_city_data" ]; then
        echo "Syncing City Arts data from /tmp/aws_seed_city_data..."
        awslocal s3 sync /tmp/aws_seed_city_data/ s3://"$AWS_CITY_ARTS_BUCKET"/
    else
        echo "Warning: /tmp/aws_seed_city_data directory not found. Skipping City Arts seeding."
    fi
else
    echo "AWS_CITY_ARTS_BUCKET is not specified. Skipping City Arts initialization."
fi

echo "--------------------------------------------------"

if [ -n "$AWS_TEST_BUCKET" ]; then
    echo "Creating Test bucket: s3://$AWS_TEST_BUCKET"
    awslocal s3 mb s3://"$AWS_TEST_BUCKET"

    if [ -d "/tmp/aws_seed_test_data" ]; then
        echo "Syncing Test data from /tmp/aws_seed_test_data..."
        awslocal s3 sync /tmp/aws_seed_test_data/ s3://"$AWS_TEST_BUCKET"/
    else
        echo "Warning: /tmp/aws_seed_test_data directory not found. Skipping Test seeding."
    fi
else
    echo "AWS_TEST_BUCKET is not specified. Skipping Test initialization."
fi

echo "=================================================="
echo "          SEEDING PROCESS COMPLETE                "
echo "=================================================="
