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
    # Create a Comprehend Client
    comprehend_client = boto3.client('comprehend')

    # Create an SSM Client
    ssm_client = boto3.client('ssm')

    # Create an S3 Client
    s3_resource = boto3.resource('s3')

    # Create a CloudWatch Events Client
    events_client = boto3.client('events')

    # Get the ARN for the Custom Entity Recognizer under training
    parameters = ssm_client.get_parameters(
        Names=['TrainingCustomEntityRecognizerARN-TCA2I',
               'ComprehendExecutionRole-TCA2I', 'CustomEntityTrainingListS3URI-TCA2I',
               'CERTrainingCompletionCheckRuleARN-TCA2I', 'CustomEntityRecognizerARN-TCA2I'],
        WithDecryption=True)

    for parameter in parameters['Parameters']:
        if parameter['Name'] == 'TrainingCustomEntityRecognizerARN-TCA2I':
            training_cer_arn = parameter['Value']
        if parameter['Name'] == 'CustomEntityTrainingListS3URI-TCA2I':
            custom_entity_training_list_s3_uri = parameter['Value']
        if parameter['Name'] == 'CERTrainingCompletionCheckRuleARN-TCA2I':
            cw_events_rule_for_this_fn = parameter['Value']
        if parameter['Name'] == 'CustomEntityRecognizerARN-TCA2I':
            original_custom_entity_recognizer_arn = parameter['Value']

    # Check Status of the comprehend custom entity Recognizer
    custom_entity_recognizer_description = comprehend_client.describe_entity_recognizer(
        EntityRecognizerArn=training_cer_arn
    )

    if custom_entity_recognizer_description['EntityRecognizerProperties']['Status'] == 'TRAINING':
        print("Amazon Comprehend Custom Entity Recognizer is still training")

    elif custom_entity_recognizer_description['EntityRecognizerProperties']['Status'] == 'SUBMITTED':
        print("Amazon Comprehend Custom Entity Recognizer training task has been submitted.")

    elif custom_entity_recognizer_description['EntityRecognizerProperties']['Status'] == 'IN_ERROR':

        # # Reset the SSM Parameter that contains the ARN for the new CER
        ssm_client.delete_parameter(Name="TrainingCustomEntityRecognizerARN-TCA2I")
        ssm_client.put_parameter(Name="TrainingCustomEntityRecognizerARN-TCA2I", Type="String", Value="NotActive")
        print("Reset Complete for SSM parameter storing training job Arn")

        # Move the Entity List file that caused the error for later analysis
        # Get the bucket name and object key for the original entity list file
        original_entity_list_object = get_s3_bucket_and_key(custom_entity_training_list_s3_uri)

        # Create the object key for updated entity list file use for training the Comprehend CER
        source_entity_list_object = original_entity_list_object
        source_entity_list_object['Key'] = prepend_to_s3_file_name(original_entity_list_object['Key'], "updated")

        # Copy the errored entity list on a separate file
        dest = s3_resource.Bucket(source_entity_list_object['Bucket'])
        dest.copy(source_entity_list_object,
                  prepend_to_s3_file_name(original_entity_list_object['Key'], "ERRORED_ENTITY_LIST", True))
        print("Moved the entity list file that caused the error")

        # Delete the previous version of the Custom Entity Recognizer
        custom_entity_recognizer_description = comprehend_client.delete_entity_recognizer(
            EntityRecognizerArn=training_cer_arn
        )
        print("Deleted Errored Custom Entity Recognizer")

        # Disable the Cloudwatch Events Rule for this function
        disable_cw_event_reponse = events_client.disable_rule(Name=cw_events_rule_for_this_fn.split('/')[-1])
        print("Disabled Cloudwatch Events Rule to check for completion of Comprehend CER Training Job")

    elif custom_entity_recognizer_description['EntityRecognizerProperties']['Status'] == 'TRAINED':
        # # Reset the SSM Parameter that contains the ARN for the new CER
        ssm_client.delete_parameter(Name="TrainingCustomEntityRecognizerARN-TCA2I")
        ssm_client.put_parameter(Name="TrainingCustomEntityRecognizerARN-TCA2I", Type="String", Value="NotActive")
        print("Reset Complete for SSM parameter storing training job Arn")

        # Move the Entity List file that caused the error for later analysis
        # Get the bucket name and object key for the original entity list file
        original_entity_list_object = get_s3_bucket_and_key(custom_entity_training_list_s3_uri)

        # Create the object key for updated entity list file use for training the Comprehend CER
        source_entity_list_object = {'Bucket': original_entity_list_object['Bucket'],
                                     'Key': prepend_to_s3_file_name(original_entity_list_object['Key'], "updated")}

        # Copy the errored entity list on a separate file
        dest = s3_resource.Bucket(source_entity_list_object['Bucket'])
        dest.copy(source_entity_list_object, original_entity_list_object['Key'])
        print("Moved the entity list file as the new default for CER Entity List")

        # # Replace the SSM Parameter that contains the ARN for the CER used in TextractComprehend Lambda
        ssm_client.delete_parameter(Name="CustomEntityRecognizerARN-TCA2I")
        ssm_client.put_parameter(Name="CustomEntityRecognizerARN-TCA2I",
                                 Type="String",
                                 Value=custom_entity_recognizer_description['EntityRecognizerProperties'][
                                     'EntityRecognizerArn'])
        print("Reset Complete for SSM parameter storing training job Arn")
        print("Updated the CER Arn SSM Parameter")

        # Delete the Previous Custom Entity Recognizer
        custom_entity_recognizer_description = comprehend_client.delete_entity_recognizer(
            EntityRecognizerArn=original_custom_entity_recognizer_arn
        )
        print("Deleted Previous Custom Entity Recognizer")

        # Disable the Cloudwatch Events Rule that triggers this Lambda Function
        disable_cw_event_reponse = events_client.disable_rule(Name=cw_events_rule_for_this_fn.split('/')[-1])
        print("Disabled Cloudwatch Events Rule to check for completion of Comprehend CER Training Job")

    else:
        print(custom_entity_recognizer_description)

    return 0


# Generate the Update Entity List's file name as defined in
# ComprehendA2I Lambda Function
def get_s3_bucket_and_key(original_entity_list_s3_uri):
    uri_with_removed_s3_prefix = original_entity_list_s3_uri.strip("s3://")

    # Separate Key and Bucket Name for Updated Entity List File
    uri_elements_list = uri_with_removed_s3_prefix.split('/')

    # Create a response object with bucket and key
    response_object = {}
    response_object['Bucket'] = uri_elements_list[0]
    response_object['Key'] = "/".join(uri_elements_list[1:])
    return response_object


# Prepend a prefix to an S3 File Name in an object key
def prepend_to_s3_file_name(object_key, prefix="", addRandom=False):
    object_key_list = object_key.split('/')
    if addRandom:
        object_key_list[-1] = prefix + "_" + str(random.randint(10000, 99999)) + "_" + object_key_list[-1]
    else:
        object_key_list[-1] = prefix + "_" + object_key_list[-1]
    return "/".join(object_key_list)