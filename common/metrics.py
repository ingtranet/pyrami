import asyncio
import fastcounter
from loguru import logger

_counters = dict()
_summaries = dict()

def metric_inc(name: str, amount: int = 1) -> None:
    if name in _counters:
        counter = _counters[name]
    else:
        counter = fastcounter.Counter()
        _counters[name] = counter
    counter.increment(amount)
    
def metric_set(name: str, value: object) -> None:
    _summaries[name] = value

async def metrics_printer():
    try:
        while True:
            metrics = dict()
            for k, v in _counters.items():
                metrics[k] = v.value
            for k, v in _summaries.items():
                metrics[k] = v
            msg = ' '.join([f'{k}={v}' for k, v in metrics.items()])
            logger.info(msg)
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        logger.info('Exiting metrics_printer...')
    except Exception as e:
        logger.error(f'Error in metrics_printer: {e}')
