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

import boto3
import json
import csv
import re

def lambda_handler(event, context):
    # Create an S3 Client
    s3_client = boto3.client('s3')

    # Create an SSM Client
    ssm_client = boto3.client('ssm')

    # Get parameters from SSM
    a2i_parameters = ssm_client.get_parameters(Names=['FlowDefARN-TCA2I',
                                                      'S3BucketName-TCA2I', 'CustomEntityTrainingListS3URI-TCA2I',
                                                      'CustomEntityTrainingDatasetS3URI-TCA2I'], WithDecryption=True)

    for parameter in a2i_parameters['Parameters']:
        if parameter['Name'] == 'FlowDefARN-TCA2I':
            hrw_arn = parameter['Value']
        elif parameter['Name'] == 'S3BucketName-TCA2I':
            primary_s3_bucket = parameter['Value']
        elif parameter['Name'] == 'CustomEntityTrainingListS3URI-TCA2I':
            custom_entities_file_uri = parameter['Value']
        elif parameter['Name'] == 'CustomEntityTrainingDatasetS3URI-TCA2I':
            custom_entities_training_data_file_uri = parameter['Value']

    s3location = ''
    if event['detail-type'] == 'SageMaker A2I HumanLoop Status Change':
        if event['detail']['flowDefinitionArn'] == hrw_arn:
            if event['detail']['humanLoopStatus'] == 'Completed':
                s3location = event['detail']['humanLoopOutput']['outputS3Uri']
            else:
                print("HumanLoop did not complete successfully")
        else:
            print("Lambda Triggered for different Human Loop Completion")
    else:
        print("Unknown Lambda Trigger")

    # If this Lambda was triggered by the textract comprehend human loop status change
    # then process further
    if s3location != '':
        s3location = s3location.replace('s3://', '')

        print("Recreating output file with human post edits...")

        # recreate the output text document, including newly identified entities.
        a2i_output_file = s3_client.get_object(Bucket=s3location[0:s3location.index('/')],
                                               Key=s3location[s3location.index('/') + 1: len(s3location)])[
            'Body'].read()
        a2i_output_file = json.loads(a2i_output_file.decode('utf-8'))

        list_of_annotated_entities = a2i_output_file['humanAnswers'][0]['answerContent']['crowd-entity-annotation'][
            'entities']

        # Check if any new custom entities were annotated by the human review
        if len(list_of_annotated_entities) > 0:

            # Get the original text that was provided to the human reviewer
            input_content = a2i_output_file['inputContent']
            original_text = input_content['originalText']

            # Create lists to hold the entities defined by the human review
            entity_text = []
            entity_type = []

            # Generate a list of unique entities annotated by Human Reviewer
            for annotated_entity in list_of_annotated_entities:
                if original_text[annotated_entity['startOffset']:annotated_entity['endOffset']] not in entity_text:
                    entity_text.append(original_text[annotated_entity['startOffset']:annotated_entity['endOffset']])
                    entity_type.append(annotated_entity['label'].upper())

            # Read the updated custom entities file and retrieve its contents
            custom_entities_file_uri = custom_entities_file_uri.replace('s3://', '')
            comprehend_data_bucket = custom_entities_file_uri[0:custom_entities_file_uri.index('/')]

            # Entity file that the last Custom Entity Model was trained on
            comprehend_entity_last_trained_file_key = custom_entities_file_uri[
                                                      custom_entities_file_uri.index('/') + 1: len(
                                                          custom_entities_file_uri)]

            # Entity file that contains the latest updates from human reviews
            temp_comprehend_entity_updated_file_key = custom_entities_file_uri[
                                                      custom_entities_file_uri.index('/') + 1: len(
                                                          custom_entities_file_uri)]
            temp_comprehend_entity_updated_file_key = temp_comprehend_entity_updated_file_key.split('/')
            temp_comprehend_entity_updated_file_key[-1] = "updated_" + temp_comprehend_entity_updated_file_key[-1]
            temp_comprehend_entity_updated_file_key = "/".join(temp_comprehend_entity_updated_file_key)
            comprehend_entity_file_key = temp_comprehend_entity_updated_file_key

            try:
                custom_entities_file = s3_client.get_object(
                    Bucket=comprehend_data_bucket,
                    Key=comprehend_entity_file_key)
                print("Latest entity file loaded")
            except:
                # Copy Object source file decalaration
                copy_source_object = {'Bucket': comprehend_data_bucket, 'Key': comprehend_entity_last_trained_file_key}
                # S3 Copy Object operation
                s3_client.copy_object(CopySource=copy_source_object, Bucket=comprehend_data_bucket,
                                      Key=comprehend_entity_file_key)

                # Try reading the file again
                custom_entities_file = s3_client.get_object(
                    Bucket=comprehend_data_bucket,
                    Key=comprehend_entity_file_key)

                print("Latest entity file loaded")

            # Read the contents of the updated custom entity file
            custom_entities_file_content = custom_entities_file['Body'].read().decode('utf-8').splitlines()

            # Remove the entities that were annotated but already exist in the model
            custom_entities_object = detect_new_entities(custom_entities_file_content, entity_text, entity_type)

            if custom_entities_object['retraining_required']:
                temp_csv_file = open("/tmp/entities_file.csv", "w+")
                temp_csv_writer = csv.writer(temp_csv_file)
                # writing the column names
                temp_csv_writer.writerow(["Text", "Type"])

                # Writing rows in to the CSV file
                for index in range(len(custom_entities_object['entity_text'])):
                    temp_csv_writer.writerow([custom_entities_object['entity_text'][index],
                                              custom_entities_object['entity_type'][index]
                                              ])
                temp_csv_file.close()

                # Create a s3 bucket resource
                s3 = boto3.resource('s3')
                comprehend_data_bucket_object = s3.Bucket(comprehend_data_bucket)
                comprehend_data_bucket_object.upload_file('/tmp/entities_file.csv', comprehend_entity_file_key)
                print("NewEntityFileUploaded")
                print("The model will be retrained")
            else:
                print("All annotated entities are already present in the training data.")


        else:
            print('No entities were annotated in the human review.')

    return 0


# Function to detect any new entities that are added and weren't already
# present in the training data for current model
def detect_new_entities(custom_entities_content, new_entity_text, new_entity_type):
    # Loop over each entity to check for its existence in the ones
    # marked by the human reviewer
    line_count = 0

    existing_entity_text_list = []
    existing_entity_type_list = []

    for entity in custom_entities_content:
        row = str(entity).replace("b'", "").replace("'", "").split(',')
        if line_count != 0 and len(row) == 2:
            if row[0] in new_entity_text:
                index_to_delete = new_entity_text.index(row[0])
                new_entity_text.pop(index_to_delete)
                new_entity_type.pop(index_to_delete)
            existing_entity_text_list.append(row[0])
            existing_entity_type_list.append(row[1])
        line_count += 1

    cleaned_values = {}
    cleaned_values['entity_text'] = existing_entity_text_list + new_entity_text
    cleaned_values['entity_type'] = existing_entity_type_list + new_entity_type
    if len(cleaned_values['entity_text']) == (len(custom_entities_content) - 2):
        cleaned_values['retraining_required'] = False
    else:
        cleaned_values['retraining_required'] = True

    return cleaned_values
