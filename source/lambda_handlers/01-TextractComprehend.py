# MIT License
#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject
# to  the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN  NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from urllib.parse import unquote_plus
import json
import boto3
import re


def lambda_handler(event, context):
    # Create an SSM Client
    ssm_client = boto3.client('ssm')

    # Create an S3 Client
    s3_client = boto3.client('s3')

    # Create a Textract Client
    textract_client = boto3.client('textract')

    # Create a Comprehend Client
    comprehend_client = boto3.client('comprehend')

    # Get the Custom Entity Recognizer's ARN from SSM Parameter Store
    comprehend_parameters = ssm_client.get_parameters(Names=['CustomEntityRecognizerARN-TCA2I',
                                                             'ComprehendExecutionRole-TCA2I',
                                                             'ComprehendTemporaryDataStoreBucketName-TCA2I'],
                                                      WithDecryption=True)

    for parameter in comprehend_parameters['Parameters']:
        if parameter['Name'] == 'CustomEntityRecognizerARN-TCA2I':
            customer_recognizer_arn = parameter['Value']
        elif parameter['Name'] == 'ComprehendExecutionRole-TCA2I':
            comprehend_execution_role_arn = parameter['Value']
        elif parameter['Name'] == 'ComprehendTemporaryDataStoreBucketName-TCA2I':
            comprehend_output_bucket = parameter['Value']

    # Iterate over all S3 Put records that have been passed to this lambda function.
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        # Send S3 Object to Textract
        response = textract_client.detect_document_text(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}})

        # Get just the filename (without input/ or trailing filetype)
        filename = ".".join(key.split(".")[:-1])
        filename = "/".join(filename.split("/")[1:])

        # Get the text blocks
        blocks = response['Blocks']

        # Save the JSON response from Textract to a folder in the S3 bucket
        raw_textract_data_response = s3_client.put_object(
            Bucket=bucket,
            Key='textract-output/raw/' + filename + '.json',
            Body=json.dumps(blocks)
        )
        print(f'Text Extraction Complete for {bucket}/{key}')

        # Recreate the raw text from the Textract Output
        raw_text = ""
        for block in blocks[1:]:
            if (block['BlockType'] == "WORD"):
                break
            raw_text = raw_text + block['Text'] + " "

        # Store it in an S3 bucket
        processed_data_key = 'textract-output/processed/' + filename + '.txt'

        # Store Processed Data in S3 Bucket
        processed_textract_data_response = s3_client.put_object(
            Bucket=bucket,
            Key=processed_data_key,
            Body=json.dumps(raw_text)
        )

        # Start the Custom Entity Recognition Job
        response = comprehend_client.start_entities_detection_job(
            InputDataConfig={
                'S3Uri': 's3://' + bucket + '/' + processed_data_key,
                'InputFormat': 'ONE_DOC_PER_FILE'
            },
            OutputDataConfig={
                'S3Uri': 's3://' + comprehend_output_bucket + '/comprehend-output/raw/'
            },
            DataAccessRoleArn=comprehend_execution_role_arn,
            JobName= re.sub(r'\W+', '', filename) + '-TextractComprehendA2I',
            EntityRecognizerArn=customer_recognizer_arn,
            LanguageCode='en'
        )

        print("Custom Entity Detection Job Started")
    return 0
