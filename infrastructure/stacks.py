# from asyncio import events
from aws_cdk import (
    Duration,
    Aws,
    Stack,
    aws_iam as _iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _event_source,
    aws_s3 as _s3,
    aws_s3_notifications as _s3_notification,
    aws_sqs as _sqs,
)
import aws_cdk as _core
from constructs import Construct


class S3SqsLambdaOpenSearchStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._region = Aws.REGION
        self._account_id = Aws.ACCOUNT_ID

        #TODO Verify Parameters
        self._domain_endpoint = _core.CfnParameter(
            self,
            'DomainEndpoint',
            default='',
            description='OpenSearch Domain Endpoint',
            type='String',
        ).value_as_string

        self._domain_arn = _core.CfnParameter(
            self,
            'DomainArn',
            default='',
            description='OpenSearch Domain ARN',
            type='String',
        ).value_as_string

        self._index_prefix = _core.CfnParameter(
            self,
            'IndexPrefix',
            default='aws-waf-log',
            description='OpenSearch Index Prefix',
            type='String',
        ).value_as_string

        # visibility_timeout must greater than function runtime
        self._queue = _sqs.Queue(self, 'Queue',
            visibility_timeout=Duration.minutes(16),
            retention_period=Duration.days(14)
        )
        self._queue_arn = self._queue.queue_arn

            
        self._sqs_event_source = _event_source.SqsEventSource(
            self._queue
        )

        self._bucket = _s3.Bucket(self, 'Bucket')
        self._bucket_arn = self._bucket.bucket_arn
        self._bucket.add_event_notification(
            _s3.EventType.OBJECT_CREATED,
            _s3_notification.SqsDestination(self._queue)
        )

        self._lambda_exec_role = self._create_lambda_role()

        # TODO install layer packages
        self._layer = _lambda.LayerVersion(
            self,'FunctionLayer',
            code=_lambda.Code.from_asset('src/layer')
        )

        self._fn = _lambda.Function(self, 'Function',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='app.handler',
            code=_lambda.Code.from_asset(
                path='src/lambda'
            ),
            timeout=Duration.minutes(15),
            memory_size=1024,
            environment={
                'REGION': self._region,
                'ACCOUNT_ID': self._account_id,
                'DOMAIN_ENDPOINT': self._domain_endpoint,
                'INDEX_PREFIX': self._index_prefix,
            },
            role=self._lambda_exec_role,
            layers=[self._layer]
        )
        self._fn.add_event_source(self._sqs_event_source )

        _core.CfnOutput(
                self, 'LambdaFunctionName',
                value=self._fn.function_name,
                description='Lambda Function Name'
        )

        _core.CfnOutput(
                self, 'SQSQueueName',
                value=self._queue.queue_name,
                description='SQS Queue Name'
        )

        _core.CfnOutput(
                self, 'S3BucketName',
                value=self._bucket.bucket_name,
                description='S3 Bucket Name'
        )

    def _create_lambda_role(self):
        _role = _iam.Role(self, 'LambdaExecRole',
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com")
        )
        _role.add_managed_policy(
            _iam.ManagedPolicy.from_managed_policy_arn(
                self, 'AWSLambdaExecutePolicy',
                'arn:aws:iam::aws:policy/AWSLambdaExecute'
            )
        )
        _role.attach_inline_policy(
            _iam.Policy(
                self, 
                "LambdaExecRoleInlinePolicy",
                statements=[
                    _iam.PolicyStatement(
                        actions=[
                            'es:DescribeDomain',
                            'es:DescribeDomains',
                            'es:DescribeDomainConfig',
                            'es:ESHttpPost',
                            'es:ESHttpPut',
                            'es:ESHttpGet'
                        ],
                        resources=[
                            self._domain_arn,
                            f'{self._domain_arn}/*'
                        ]
                    ),
                    _iam.PolicyStatement(
                        actions=[
                            's3:GetObject*',
                        ],
                        resources=[
                            self._bucket_arn,
                            f'{self._bucket_arn}/*'
                        ]
                    ),
                    _iam.PolicyStatement(
                        actions=[
                            'sqs:*',
                        ],
                        resources=[
                            self._queue_arn
                        ]
                    ),
                ]
            )
        )
        return _role 

