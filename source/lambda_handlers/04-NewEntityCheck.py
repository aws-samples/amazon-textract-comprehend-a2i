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

import json
import boto3
import random


def lambda_handler(event, context):
    # Create an S3 Client
    s3_client = boto3.client('s3')

    # Create an SSM Client
    ssm_client = boto3.client('ssm')

    # Create a Cloudwatch Events Client
    events_client = boto3.client('events')

    # Create a Comprehend Client
    comprehend_client = boto3.client('comprehend')

    # Get parameters from SSM
    parameters = ssm_client.get_parameters(Names=['CustomEntityRecognizerARN-TCA2I',
                                                  'CERTrainingCompletionCheckRuleARN-TCA2I',
                                                  'CustomEntityTrainingListS3URI-TCA2I',
                                                  'ComprehendExecutionRole-TCA2I',
                                                  'CustomEntityTrainingDatasetS3URI-TCA2I'],
                                           WithDecryption=True)

    for parameter in parameters['Parameters']:
        if parameter['Name'] == 'CustomEntityRecognizerARN-TCA2I':
            custom_entity_recognizer = parameter['Value']
        elif parameter['Name'] == 'CERTrainingCompletionCheckRuleARN-TCA2I':
            cw_events_rule_for_training_completion_check_lambda = parameter['Value']
        elif parameter['Name'] == 'CustomEntityTrainingDatasetS3URI-TCA2I':
            custom_entities_training_data_file_uri = parameter['Value']
        elif parameter['Name'] == 'CustomEntityTrainingListS3URI-TCA2I':
            custom_entities_file_uri = parameter['Value']
        elif parameter['Name'] == 'ComprehendExecutionRole-TCA2I':
            comprehend_execution_role = parameter['Value']

    # Read the updated custom entities file and retrieve its contents
    custom_entities_file_uri = custom_entities_file_uri.replace('s3://', '')
    comprehend_data_bucket = custom_entities_file_uri[0:custom_entities_file_uri.index('/')]

    # Entity file that the last Custom Entity Model was trained on
    last_trained_custom_entities_file_key = custom_entities_file_uri[
                                            custom_entities_file_uri.index('/') + 1: len(custom_entities_file_uri)]

    # Entity file that contains the latest updates from human reviews
    temp_comprehend_entity_updated_file_key = custom_entities_file_uri[
                                              custom_entities_file_uri.index('/') + 1: len(custom_entities_file_uri)]
    temp_comprehend_entity_updated_file_key = temp_comprehend_entity_updated_file_key.split('/')
    temp_comprehend_entity_updated_file_key[-1] = "updated_" + temp_comprehend_entity_updated_file_key[-1]
    temp_comprehend_entity_updated_file_key = "/".join(temp_comprehend_entity_updated_file_key)
    hrw_updated_custom_entities_file_key = temp_comprehend_entity_updated_file_key

    # Read the Last Custom Entities file the Comprehend Model was training upon
    last_trained_custom_entities_file = s3_client.get_object(
        Bucket=comprehend_data_bucket,
        Key=last_trained_custom_entities_file_key)

    # Read the Last Updated Custom Entities file
    hrw_updated_custom_entities_file = s3_client.get_object(
        Bucket=comprehend_data_bucket,
        Key=hrw_updated_custom_entities_file_key)

    print("Latest entity files loaded")

    # Read the contents of the last custom entity file used for model training
    last_trained_custom_entities_content = last_trained_custom_entities_file['Body'].read().split(b'\n')

    # Read the contents of the updated custom entity file
    hrw_updated_custom_entities_content = hrw_updated_custom_entities_file['Body'].read().split(b'\n')

    if check_for_new_entities(last_trained_custom_entities_content, hrw_updated_custom_entities_content):
        print("New entities found. Retraining the model")

        entity_types = get_entity_types(hrw_updated_custom_entities_content)

        # Call the Comprehend Create Entity Recognizer API
        custom_entity_recognizer_response = comprehend_client.create_entity_recognizer(
            RecognizerName="Text-Analysis-Custom-Entity-Recognizer" + str(random.randint(100000, 999999)),
            DataAccessRoleArn=comprehend_execution_role,
            InputDataConfig={
                "EntityTypes": entity_types,
                "Documents": {
                    "S3Uri": custom_entities_training_data_file_uri
                },
                "EntityList": {
                    "S3Uri": "s3://" + comprehend_data_bucket + "/" + hrw_updated_custom_entities_file_key
                }

            },
            LanguageCode="en"
        )

        # Extract the ARN of the new Custom Entity Recognizer from the response object
        training_cer_arn = custom_entity_recognizer_response['EntityRecognizerArn']

        # # Code to set the new under-training CER parameter
        ssm_client.delete_parameter(Name="TrainingCustomEntityRecognizerARN-TCA2I")
        ssm_client.put_parameter(Name="TrainingCustomEntityRecognizerARN-TCA2I",
                                 Type="String", Value=training_cer_arn)

        # Enable the Cloudwatch Events Rule that looks for CER Training Completion
        enable_cw_event_reponse = events_client.enable_rule(
            Name=cw_events_rule_for_training_completion_check_lambda.split('/')[-1])
        print("Enabled Cloudwatch Events Rule to check for completion of Comprehend CER Training Job")

    else:
        print("No new entities since the last model retraining")

    return 0


# Function the compares the difference between the two entity lists
# Returns: BOOLEAN
def check_for_new_entities(last_trained_custom_entities, hrw_updated_custom_entities):
    list_of_last_trained_entities = []
    list_of_hrw_updated_entities = []

    line_count = 0
    for entity in last_trained_custom_entities:
        row = str(entity).replace("b'", "").replace("'", "").split(',')
        if line_count != 0 and len(row) == 2:
            list_of_last_trained_entities.append(row[0])
        line_count += 1

    line_count = 0
    for entity in hrw_updated_custom_entities:
        row = str(entity).replace("b'", "").replace("'", "").split(',')
        if line_count != 0 and len(row) == 2:
            list_of_hrw_updated_entities.append(row[0])
        line_count += 1

    if len(list(set(list_of_hrw_updated_entities) - set(list_of_last_trained_entities))) == 0:
        return False
    else:
        return True
    return 0


def get_entity_types(hrw_updated_custom_entities):
    list_of_hrw_updated_entities = []

    line_count = 0
    for entity in hrw_updated_custom_entities:
        row = str(entity).replace("b'", "").replace("'", "").split(',')
        if line_count != 0 and len(row) == 2:
            list_of_hrw_updated_entities.append(row[1][:-2].upper())
        line_count += 1

    response_object = []
    for i in list(set(list_of_hrw_updated_entities)):
        response_object.append({"Type": i})

    return response_object