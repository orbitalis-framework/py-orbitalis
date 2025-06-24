import unittest

from busline.local.local_pubsub_client import LocalPubSubClientBuilder

from orbitalis.plugin.configuration import PluginConfiguration
from orbitalis.plugin.plugin import Plugin


class MockPlugin(Plugin):
    pass


class TestPlugin(unittest.IsolatedAsyncioTestCase):


    def setUp(self):
        self.plugin = MockPlugin(
            eventbus_client=LocalPubSubClientBuilder()\
                    .with_default_publisher()\
                    .with_closure_subscriber(lambda t, e: ...)\
                    .build()
        )

    async def test_discover(self):
        await self.plugin.start()


