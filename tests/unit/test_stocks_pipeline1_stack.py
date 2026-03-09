import aws_cdk as core
import aws_cdk.assertions as assertions

from stocks_pipeline1.stocks_pipeline1_stack import StocksPipeline1Stack

# example tests. To run these tests, uncomment this file along with the example
# resource in stocks_pipeline1/stocks_pipeline1_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = StocksPipeline1Stack(app, "stocks-pipeline1")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
