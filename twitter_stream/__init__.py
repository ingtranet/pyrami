import asyncio
import sys

from dataclasses import dataclass, field
from typing import List

import anyio
import msgpack

from loguru import logger
from zstandard import ZstdCompressor

from common.collections import LeveledQueue
from common.kinesis import KinesisPublisher
from common.metrics import metric_inc, metrics_printer
from common.util import signal_handler
from twitter_stream.twitter import TwitterClient

@dataclass
class Config:
    BEARER_TOKEN: str
    API_URL: str = 'https://api.twitter.com/2/tweets/sample/stream'
    KINESIS_STREAM: str = 'test'
    KAFKA_TOPIC: str = 'test'
    MEM_QUEUE_SIZE: int = 256
    LOG_LEVEL: str = 'INFO'
    #EXPENSIONS: List[str] = field(default_factory=list)
    #MEDIA_FIELDS: List[str] = field(default_factory=list)
    #PLACE_FIELDS: List[str] = field(default_factory=list)
    #POLL_FIELDS: List[str] = field(default_factory=list)
    #TWEET_FIELDS: List[str] = field(default_factory=list)
    #USER_FIELDS: List[str] = field(default_factory=list)

class StreamWorker:
    def __init__(self, config: Config, queue: LeveledQueue) -> None:
        self.config = config
        self.twitter_client = TwitterClient(config.BEARER_TOKEN)
        self.compressor = ZstdCompressor()
        self.queue = queue

    async def run(self, scope: anyio.CancelScope):
        try:
            async with self.twitter_client.get_stream() as response:
                if response.status_code != 200:
                    if response.status_code == 429:
                        await anyio.sleep(30)
                    raise RuntimeError(f'HTTP Failed: {response}')
                
                aiter_lines = response.aiter_lines()
                while True:
                    with anyio.fail_after(60):
                        line = await aiter_lines.__anext__()
                    line = line.strip()
                    if not line:
                        continue
                    line = line.encode()
                    data = {'topic': self.config.KAFKA_TOPIC, 'key': None, 'value': line}
                    data = msgpack.packb(data)
                    data = self.compressor.compress(data)
                    self.queue.push(data)
                    metric_inc('tweets_input')
        except BaseException as e:
            logger.error(f'StreamWorker stopped: {e.__class__}:{e}')
            scope.cancel()


class PublishWorker:
    def __init__(self, config: Config, queue: LeveledQueue) -> None:
        self.config = config
        self.queue = queue
        self.kinesis_publisher = KinesisPublisher(config.KINESIS_STREAM)

    async def run(self, scope: anyio.CancelScope):
        buffer = list()
        with anyio.CancelScope(shield=True):
            while (not scope.cancel_called) or (not self.queue.empty()):
                with anyio.move_on_after(1):
                    buffer.append(await self.queue.pop())
                if len(buffer) >= 100 or (scope.cancel_called and self.queue.empty() and len(buffer) != 0):
                    await self.kinesis_publisher.publish_async(buffer)
                    metric_inc('kinesis_publish', len(buffer))
                    buffer.clear()
        logger.info('PublishWorker stopped')

async def async_run(config: Config):
    queue = LeveledQueue(config.MEM_QUEUE_SIZE)
    stream_worker = StreamWorker(config, queue)
    publish_worker = PublishWorker(config, queue)
    
    metric_worker = asyncio.Task(metrics_printer())
    async with anyio.create_task_group() as tg:
        tg.start_soon(signal_handler, tg.cancel_scope)
        tg.start_soon(stream_worker.run, tg.cancel_scope)
        tg.start_soon(publish_worker.run, tg.cancel_scope)
    await queue.close()
    metric_worker.cancel()
    

def run(settings: dict):
    config = Config(**settings)
    logger.remove()
    logger.add(sys.stderr, level=config.LOG_LEVEL)    
    anyio.run(async_run, config, backend_options={'use_uvloop': False})
 

    