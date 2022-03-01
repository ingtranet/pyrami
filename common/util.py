from signal import SIGTERM, SIGINT, strsignal

import anyio
from loguru import logger

async def signal_handler(scope: anyio.CancelScope):
    with anyio.open_signal_receiver(SIGINT, SIGTERM) as signals:
        async for signum in signals:
            logger.warning(f'Signal received: {strsignal(signum)}. Cancelling scope...')
            scope.cancel()
            return