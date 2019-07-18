import asyncio
import functools
from .state import State


class Node:
    """Raft Node (Server)"""

    def __init__(self, address, loop):
        self.host, self.port = address
        self.cluster = set()

        self.loop = loop
        self.state = State(self)

    async def start(self):
        self.state.start()

    def stop(self):
        self.state.stop()

    def on_follower(self):
        pass

    def on_leader(self):
        pass

    async def async_on_follower(self):
        pass

    async def async_on_leader(self):
        pass

    def update_cluster(self, address_list):
        self.cluster.update({address_list})

    @property
    def cluster_count(self):
        return len(self.cluster)

    def request_handler(self, data):
        self.state.request_handler(data)

    async def send(self, data, destination):
        raise NotImplementedError

    def broadcast(self, data):
        """Sends data to all Nodes in cluster (cluster list does not contain self Node)"""
        for destination in self.cluster:
            asyncio.ensure_future(self.send(data, destination), loop=self.loop)
