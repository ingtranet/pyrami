import tempfile
import shutil
from asyncio import Queue
from queuelib import FifoDiskQueue

class LeveledQueue:
    def __init__(self, mem_maxsize=128) -> None:
        self.temp_path = tempfile.mkdtemp()
        self.mem_queue = Queue(maxsize=mem_maxsize)
        self.disk_queue = FifoDiskQueue(self.temp_path)
    
    async def close(self):
        self.disk_queue.close()
        try:
            shutil.rmtree(self.temp_path)
        except:
            pass

    def push(self, bin: bytes):
        if self.mem_queue.full():
            self.disk_queue.push(bin)
        else:
            self.mem_queue.put_nowait(bin)

    def empty(self) -> bool:
        return self.mem_queue.empty()
    
    async def pop(self) -> bytes:
        item = await self.mem_queue.get()
        if disk_obj := self.disk_queue.pop():
            self.mem_queue.put_nowait(disk_obj)
        return item
    
    def size(self) -> int:
        return self.mem_queue.qsize() + self.disk_queue.info['size']