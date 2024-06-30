import aws_cdk as cdk
from infrastructure.cdk_stack import LogProcessingStack

app = cdk.App()
LogProcessingStack(app, "LogProcessingStack")
app.synth()
