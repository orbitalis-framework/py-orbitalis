import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from busline.client.pubsub_client import PubTopicSubClient
import uuid


@dataclass(kw_only=True)
class Orbiter(ABC):
    """

    Author: Nicola Ricciardi
    """

    eventbus_client: PubTopicSubClient

    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))


    def discard_expired_pending_requests(self, /, expiration_date: Optional[datetime] = None, seconds: Optional[float] = None) -> int:
        """
        Remove from pending requests expired requests based on datetime provided or seconds elapsed.
        Seconds override expiration_date.
        Return total amount of discarded requests
        """

        if expiration_date is None and seconds is None:
            raise ValueError("Provided at least one parameter")

        expiration_date = datetime.now() - timedelta(seconds=seconds)

        n = 0
        for identifier, pending_req in self.pending_requests.items():
            if pending_req.when < expiration_date:
                del self.pending_requests[identifier]
                n += 1
                continue

        return n


    async def start(self, *args, **kwargs):
        logging.info(f"{self}: starting...")
        await self.on_starting(*args, **kwargs)
        await self._internal_start(*args, **kwargs)
        await self.on_started(*args, **kwargs)
        logging.info(f"{self}: started")


    async def on_starting(self, *args, **kwargs):
        """
        TODO
        """

    async def _internal_start(self, *args, **kwargs):
        """
        TODO
        """

        await self.eventbus_client.connect()

    async def on_started(self, *args, **kwargs):
        """
        TODO
        """

    async def stop(self, *args, **kwargs):
        logging.info(f"{self}: stopping...")
        await self.on_stopping(*args, **kwargs)
        await self._internal_stop(*args, **kwargs)
        await self.on_stopped(*args, **kwargs)
        logging.info(f"{self}: stopped")


    async def on_stopping(self, *args, **kwargs):
        """
        TODO
        """

    async def _internal_stop(self, *args, **kwargs):
        """
        TODO
        """

    async def on_stopped(self, *args, **kwargs):
        """
        TODO
        """

    def __repr__(self) -> str:
        return self.identifier


