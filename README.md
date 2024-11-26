# nextchapter-import-worker

## Overview

The `nextchapter-import-worker` is an AWS Lambda function designed to process book data from CSV files stored in an S3 bucket. It fetches additional book information from external APIs and updates a backend service with the processed data.

## Features

- **S3 Integration**: Downloads CSV files from a specified S3 bucket.
- **API Integration**: Fetches book details using the Google Books API.
- **Backend Updates**: Posts processed book data to a backend service.
- **Environment Configuration**: Utilizes environment variables for configuration and sensitive data.

## Environment Variables

The function uses the following environment variables:

- `BASE_URL`: The base URL of the backend service.
- `REFRESH_TOKEN`: Token used to fetch an access token for API authentication.
- `DEBUG`: Enables debug logging when set to a truthy value.

## Setup

1. **AWS Lambda Configuration**:
   - Set up the Lambda function with the necessary environment variables.
   - Ensure the function has permissions to access the S3 bucket and make HTTP requests.

2. **S3 Bucket**:
   - Store your CSV files in the specified S3 bucket.
   - Ensure the bucket is configured to trigger the Lambda function on new file uploads.

3. **Dependencies**:
   - The function requires `boto3` for AWS interactions. Ensure this is included in your Lambda deployment package.

## Development

To test the function locally, you can use the provided `event.json` file to simulate S3 events.
