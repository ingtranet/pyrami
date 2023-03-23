from hashlib import sha1
from asyncio import Queue
from typing import Any
from enum import Enum
from datetime import timedelta

import anyio
from anyio.abc._tasks import TaskGroup
import boto3
from loguru import logger
from redis import Redis

class KinesisPublisher:
    def __init__(self, stream_name: str, no_retry: bool = False) -> None:
        logger.info(f'Initializing KinesisPublisher with stream_name={stream_name}')
        self.stream_name = stream_name
        self.no_retry = no_retry
        self.client = boto3.client('kinesis')

    def publish(self, records):
        logger.debug(f'Publishing {len(records)} records...')
        success = False
        records = [
            {
                'Data': d,
                'PartitionKey': sha1(d).hexdigest()
            } for d in records
        ]
        while not success:
            try:
                result = self.client.put_records(StreamName=self.stream_name, Records=records)
                if result['FailedRecordCount'] != 0:
                    raise RuntimeError('Publishing failed for some rows')
                success=True
            except Exception as e:
                logger.error(f'Publish failed: {e}')
                if self.no_retry:
                    raise e
    
    async def publish_async(self, records):
        await anyio.to_thread.run_sync(self.publish, records)


class ShardIteratorType(Enum):
    AT_SEQUENCE_NUMBER = 'AT_SEQUENCE_NUMBER'
    AFTER_SEQUENCE_NUMBER = 'AFTER_SEQUENCE_NUMBER'
    AT_TIMESTAMP = 'AT_TIMESTAMP'
    TRIM_HORIZON = 'TRIM_HORIZON'
    LATEST = 'LATEST'



class KinesisConsumer:
    def __init__(self, stream_name: str, consumer_name: str, redis_url: str, limit: int = 1024, interval: int = 1, from_earliest=False) -> None:
        logger.info(f'Initializing KinesisConsumer with stream_name={stream_name}')
        self.stream_name = stream_name
        self.consumer_name = consumer_name
        self.limit = limit
        self.interval = interval
        self.from_earliest = from_earliest
        self.client = boto3.client('kinesis')
        r_host, r_port = redis_url.split(':')
        self.redis_client = Redis(host=r_host, port=r_port)
        self.send_stream, self.receive_stream = anyio.create_memory_object_stream()
        self.latest_seq_numbers = dict()

    def get_shard_ids(self):
        shards = self.client.list_shards(StreamName=self.stream_name)
        return [s['ShardId'] for s in shards.get('Shards', [])]

    async def get_shard_ids_async(self):
        return await anyio.to_thread.run_sync(self.get_shard_ids)

    def get_shard_iterator(self, shard_id: str, shard_iterator_type: ShardIteratorType, sequnce_number: int = None):
        if sequnce_number:
            return self.client.get_shard_iterator(StreamName=self.stream_name, ShardId=shard_id, ShardIteratorType=shard_iterator_type.value, StartingSequenceNumber=sequnce_number)['ShardIterator']
        else:
            return self.client.get_shard_iterator(StreamName=self.stream_name, ShardId=shard_id, ShardIteratorType=shard_iterator_type.value)['ShardIterator']

    async def get_shard_iterator_async(self, shard_id: str, shard_iterator_type: ShardIteratorType, sequnce_number: int = None):
        return await anyio.to_thread.run_sync(self.get_shard_iterator, shard_id, shard_iterator_type, sequnce_number)

    def get_records(self, shard_iterator: str):
        return self.client.get_records(ShardIterator=shard_iterator, Limit=self.limit)

    async def get_records_async(self, shard_iterator: str):
        return await anyio.to_thread.run_sync(self.get_records, shard_iterator)

    async def mark_sequence_number(self, shard_id: str, sequence_number: str):
        logger.debug(f'Marking sequence number.. shard_id={shard_id} seq_no={sequence_number}')
        key = f'{self.consumer_name}/{self.stream_name}/{shard_id}'
        return await anyio.to_thread.run_sync(self.redis_client.setex, key, timedelta(weeks=4), sequence_number.encode())

    async def get_marks(self, shard_id: str) -> str:
        key = f'{self.consumer_name}/{self.stream_name}/{shard_id}'
        value = await anyio.to_thread.run_sync(self.redis_client.get, key)
        return value.decode() if value else None

    async def read_shard(self, shard_id: str, shard_iterator: str, tg: TaskGroup):
        while True:
            try:
                response = await self.get_records_async(shard_iterator)
                for record in response['Records']:
                    record['ShardId'] = shard_id
                    await self.send_stream.send(record)

                if next_iter := response.get('NextShardIterator'):
                    logger.debug(f'Next iterator exists. Sleeping for {self.interval} seconds...')
                    shard_iterator = next_iter
                    await anyio.sleep(self.interval)
                elif child_shards := response.get('ChildShards'):
                    shard_ids = [shard['ShardId'] for shard in child_shards]
                    logger.debug(f'New child shards exists. Spawning readers for {shard_ids}')
                    shard_iters = [await self.get_shard_iterator_async(sid, ShardIteratorType.TRIM_HORIZON) for sid in shard_ids]
                    for sit in shard_iters:
                        tg.start_soon(self.read_shard, sit)
                elif len(response['Records']) == 0:
                    logger.debug(f'Record Not exists. Sleeping for {self.interval} seconds...')
                    await anyio.sleep(self.interval)
            except Exception as e:
                logger.error(f'Error while reading shard: {e}')
                await self.send_stream.aclose()
                tg.cancel_scope.cancel()

    async def get_iterator(self):
        shard_ids = await self.get_shard_ids_async()
        logger.debug(f'Initializing an iterator for {shard_ids}')
        marks = [await self.get_marks(sid) for sid in shard_ids]

        shard_iters = list()
        for sid, mark in zip(shard_ids, marks):
            if mark:
                logger.debug(f'Acquiring shard iterator for {sid} after {mark}')
                iterator = await self.get_shard_iterator_async(sid, ShardIteratorType.AFTER_SEQUENCE_NUMBER, mark)
            else:
                if self.from_earliest:
                    logger.debug(f'Acquiring shard iterator for {sid} from beginning')
                    iterator = await self.get_shard_iterator_async(sid, ShardIteratorType.TRIM_HORIZON)
                else:
                    logger.debug(f'Acquiring shard iterator for {sid} from latest')
                    iterator = await self.get_shard_iterator_async(sid, ShardIteratorType.LATEST)
            shard_iters.append(iterator)

        async with anyio.create_task_group() as tg:
            async with self.send_stream:
                async with self.receive_stream:
                    for sid, sit in zip(shard_ids, shard_iters):
                        tg.start_soon(self.read_shard, sid, sit, tg)
                    while not tg.cancel_scope.cancel_called:
                        record = await self.receive_stream.receive()
                        seq_no = record['SequenceNumber']
                        shard_id = record['ShardId']
                        self.latest_seq_numbers[shard_id] = max(self.latest_seq_numbers.get(shard_id, ''), seq_no)
                        try:
                            yield record['Data']
                        except GeneratorExit:
                            tg.cancel_scope.cancel()

    async def commit(self):
        for k, v in self.latest_seq_numbers.items():
            async with anyio.create_task_group() as tg:
                tg.start_soon(self.mark_sequence_number, k, v)


