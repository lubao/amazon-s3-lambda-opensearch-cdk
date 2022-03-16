import aws_cdk as core
import aws_cdk.assertions as assertions

from amazon_s3_lambda_opensearch.amazon_s3_lambda_opensearch_stack import AmazonS3LambdaOpensearchStack

# example tests. To run these tests, uncomment this file along with the example
# resource in amazon_s3_lambda_opensearch/amazon_s3_lambda_opensearch_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AmazonS3LambdaOpensearchStack(app, "amazon-s3-lambda-opensearch")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
