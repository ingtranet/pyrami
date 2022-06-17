from dataclasses import dataclass
import sys
from aiokafka import AIOKafkaProducer
import anyio

from loguru import logger
import msgpack
from zstandard import ZstdDecompressor


from common.kinesis import KinesisConsumer
from common.metrics import metric_inc, metrics_printer
from common.util import signal_handler


@dataclass
class Config:
    STREAM_NAME: str
    CONSUMER_NAME: str
    REDIS_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str
    LOG_LEVEL: str = 'INFO'
    COMMIT_FOR_EVERY: int = 1024
    FROM_EARLIEST: str = 'false'


class StreamWorker:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.kinesis_client = KinesisConsumer(config.STREAM_NAME, config.CONSUMER_NAME, config.REDIS_URL, limit=2048, from_earliest=(config.FROM_EARLIEST == 'true'))
        self.kafka_producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            compression_type='gzip'
        )
    
    async def run(self):
        await self.kafka_producer.start()
        decompressor = ZstdDecompressor()
        wait_for_commit = 0
        try:
            async for record in self.kinesis_client.get_iterator():
                    metric_inc('got_from_kinesis')
                    record = decompressor.decompress(record)
                    record = msgpack.unpackb(record)
                    await self.kafka_producer.send(record['topic'], record['value'], record['key'])
                    metric_inc('sent_to_kafka')
                    wait_for_commit += 1
                    if wait_for_commit >= self.config.COMMIT_FOR_EVERY:
                        await self.kafka_producer.flush()
                        await self.kinesis_client.commit()
                        metric_inc('commit', wait_for_commit)
                        wait_for_commit = 0
        except BaseException as e:
            logger.error(f'StreamWorker stopped: {e.__class__}:{e}')
            with anyio.CancelScope(shield=True):
                await self.kafka_producer.stop()
                await self.kinesis_client.commit()

async def run_async(config: Config):
    stream_worker = StreamWorker(config)
    async with anyio.create_task_group() as tg:
        tg.start_soon(signal_handler, tg.cancel_scope)
        tg.start_soon(metrics_printer)
        tg.start_soon(stream_worker.run)


def run(settings: dict):
    config = Config(**settings)
    logger.remove()
    logger.add(sys.stderr, level=config.LOG_LEVEL)    
    anyio.run(run_async, config)