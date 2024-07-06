# Log Processing and Monitoring System

This project implements a serverless log processing and monitoring system using AWS services including S3, Lambda, SQS, SNS, and CloudWatch.

## Prerequisites

- AWS Account
- AWS CLI installed and configured
- Python 3.9 or later
- AWS CDK installed

## Setup and Deployment

1. Clone the repository:
git clone [repository-url]
cd [project-directory]

2. Create and activate a virtual environment:
python -m venv .venv
source .venv/bin/activate / On Windows, use .venv\Scripts\activate

3. Install the required dependencies:
pip install -r requirements.txt

4. Deploy the CDK stack:
cdk deploy

## Usage

1. Set your environment variables

2. Put your logs in data/<your_log_file>

3. Run "python3 scripts/send_data.py"

After this, your data will be sent to the s3 bucket. 

Lambda functions will process the logs and put important metadata for them in the "processed logs" s3 bucket. 

They will also send you SNS notifications if there is unusually high traffic or lots of error codes in the logs.