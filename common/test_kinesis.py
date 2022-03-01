import os

import pytest
pytestmark = pytest.mark.anyio

import boto3
from moto import mock_kinesis

from common.kinesis import KinesisPublisher

@pytest.fixture(scope='function')
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='function')
def kinesis_publisher(aws_credentials):
    with mock_kinesis():
        stream_name = 'good-stream'
        client = boto3.client('kinesis')
        client.create_stream(
            StreamName=stream_name,
            ShardCount=123,
            StreamModeDetails={
                'StreamMode': 'ON_DEMAND'
            }
        )
        yield KinesisPublisher(stream_name, no_retry=True)


def test_kinesis_publish(kinesis_publisher: KinesisPublisher):
    kinesis_publisher.publish([b'test', b'test2', b'test3'])

