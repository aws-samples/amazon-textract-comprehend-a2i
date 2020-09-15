# Document Analysis Solution using Amazon Textract, Amazon Comprehend and Amazon A2I

As business grow, there is a commensurate growth in the number of documents that need to be analyzed.
Traditionally, businesses had to manually review documents as they came in, but now, this process can be
automated with help of Machine Learning and Artificial Intelligence technologies in Amazon Web Services.


## Introduction

This solution uses Amazon Textract, Amazon Comprehend and Amazon A2I to deploy an
end-to-end document analysis solution. This solution takes documents in the for of scanned images
as input performs the following steps on them:

1. Amazon Textract to extract the text from the uploaded images
2. Recreate the document (in text format) using an AWS Lambda Function
3. Use Amazon Comprehend to analyse the text and perform Custom Named Entity Recognition on the extracted text
4. Use Amazon Augmented AI (Amazon A2I) to send the extract entities and the text to a human reviewer for a human review for any missing entities
5. Collate the results and feedback provided by the human reviewer and retrain the model every time new custom entities are detected.

## Solution Architecture

![](./images/Solution-Architecture.png)

## Setup Resources
In order to setup the required resources for this solution, you need to execute the Cloudformation Template that is provided with the solution. Once you have completed the prerequisites listed below, you should be able to deploy the Cloudformation Template in your account.

### Prerequisites

There are 2 prerequisite steps that you need to perform before you can
deploy this solution to your AWS account and here are the steps to do them:

####  Create a Human Review Workflow

* Create an S3 Bucket called **S3BucketNamePlaceholder** in the region that you intend to deploy this solution.

  Your bucket name should be **different than the one above and unique in the region** but this tutorial will use **S3BucketNamePlaceholder** when it references to this bucket.

* Create a [private workforce](https://docs.aws.amazon.com/sagemaker/latest/dg/sms-workforce-management-private-console.html) from the Amazon Sagemaker console.

* Create a [Custom Worker Template](https://docs.aws.amazon.com/sagemaker/latest/dg/a2i-create-worker-template-console.html) from Amazon Augmented AI console. Use the file **/code/ui/translate_template.html** as the custom template.

* Create a [Flow Definition](https://docs.aws.amazon.com/sagemaker/latest/dg/a2i-create-flow-definition.html#a2i-create-human-review-console) from the Amazon Augmented AI console.
    1. Use **S3BucketNamePlaceholder** for S3 Bucket.
    2. For IAM role, choose 'Create a new role'.
    3. For Task Type, choose 'Custom'
    4. For Template, choose the custom worker template you created in step 4 above.
    5. For Worker Types, choose 'Private'.
    6. For Private Teams, choose the team you created in step 3 above.
    7. After creation, copy the **Flow Definition ARN**. We will use it later.

#### Train the Comprehend Custom Entity Model

* Go to S3 console, and create a folder called **comprehend-data** in **S3BucketNamePlaceholder** Bucket.

* Copy the contents of the folder `./comprehend-cer-resources/` in the **comprehend-data** folder.

* Go to Amazon Comprehend and [train a custom entity recognizer](https://aws.amazon.com/blogs/machine-learning/build-a-custom-entity-recognizer-using-amazon-comprehend/)
using the files you just copied in **comprehend-data** folder.


### Parameters

There are several parameters that you would need to deploy the Cloudformation
template to your account. This section lists those parameters so
that you understand them and get prepared for deploying the Cloudformation Template.

#### S3BucketName
Use the same bucket that you have used so far and enter **S3BucketNamePlaceholder** in this box.

#### CustomEntityTrainingDatasetS3URI
The dataset that will be used for training the Amazon Comprehend Custom Entity Recognizer. For this example, a single file contains all the training data, one document per line. An example document is available in `./comprehend-cer-resources/raw_text.csv'

#### CustomEntityTrainingListS3URI
This document contains the list of Custom Entities as shown [here](https://docs.aws.amazon.com/comprehend/latest/dg/cer-entity-list.html). An example file can be found in `./comprehend-cer-resources/entity_list.csv'

#### CustomEntityRecognizerArn
Train a Custom Entity Recognizer using the documents above as shown [here](https://docs.aws.amazon.com/comprehend/latest/dg/training-recognizers.html). Once you have trained the Custom Entity Recognizer, you can use the ARN as a parameter to the Cloudformation template.

#### FlowDefinitionARN
Use the task template available at `./ui/task-template.html` to create a Custom Human Review workflow as shown [here](https://docs.aws.amazon.com/sagemaker/latest/dg/a2i-task-types-custom.html). Once the workflow becomes available to use, the ARN of this workflow can be used as **FlowDefinitionARN** parameter.

#### S3ComprehendBucketName
Provide any random string to be a bucket name (which needs to be unique in the region). This string will be used as the bucket name for storing the data generated by Amazon Comprehend temporarily. **You don't need to create this bucket. The Cloudformation template will create it and manage it for you.**

### Package and Deploy the Cloudformation Template

#### Create an S3 Bucket for storing packaged Cloudformation Resources

Create an S3 Bucket by the name **CFN-RESOURCES-123456789** (replace **CFN-RESOURCES-123456789** with your own bucket name) to host your Cloudformation resources.

#### Package the Cloudformation Template

Package the Cloudformation Template and code for the Lambdas using the following command:

`aws cloudformation package --template-file ./source/Textract-Comprehend-A2I.yaml --output-template-file ./source/CFN-Output-Textract-Comprehend-A2I.yaml --s3-bucket CFN-RESOURCES-123456789 --region us-east-1 --force-upload`

#### Deploy the Cloudformation Template

Now that you have packaged your code you can run the following command to deploy the Cloudformation Template.

`aws cloudformation deploy --template-file ./source/CFN-Output-Textract-Comprehend-A2I.yaml --region us-east-1 --capabilities CAPABILITY_IAM --stack-name <INSERT STACK NAME HERE> --parameter-overrides S3BucketName=<INSERT PARAMETER HERE> FlowDefinitionARN=<INSERT PARAMETER HERE> CustomEntityRecognizerARN=<INSERT PARAMETER HERE> S3ComprehendBucketName=<INSERT PARAMETER HERE> CustomEntityTrainingListS3URI=<INSERT PARAMETER HERE> CustomEntityTrainingDatasetS3URI=<INSERT PARAMETER HERE>`

## Test the Deployment

Go to the S3 Bucket, **S3BucketNamePlaceholder**, and create a new folder titled **input**. Whenever you will upload a .jpeg, or a .png file in this folder, the workflow will begin.

Log in to your A2I Review Console and make an desired changes.

## Results

At the end of each day, a Cloudwatch Event invokes a Lambda function automatically
that looks for any new custom entities that the human reviewers may have identified using the Amazon A2I worker portal. If there are any identities that did not
exist in the entity list file that was used to train the model, then it
retrains the Amazon Comprehend Custom Entity model and uses the new model for inference thereafter.

This automated retraining process, allows the model to improve perpetually and
requires lesser human intervention over time, hence saving time and cost for your business.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
