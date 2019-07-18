from .replicator import Replicated, ReplicatedDict, ReplicatedList
from .state import State


__all__ = [
    'Replicated',
    'ReplicatedDict',
    'ReplicatedList',
    'get_leader',
    'wait_until_leader'
]


get_leader = State.get_leader
wait_until_leader = State.wait_until_leader
