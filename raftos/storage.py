import os


class IPersistentStorage(object):

    def get_content(self, key):
        raise NotImplementedError

    def write_content(self, key, val):
        raise NotImplementedError


class ICacheStorage(IPersistentStorage):
    """Persistent dict-like storage on a disk accessible by obj['item_name']"""

    def __init__(self, id):
        self._id = id
        self.cache = {}

    def update(self, kwargs):
        raise NotImplementedError

    def exists(self, name):
        raise NotImplementedError

    def __getitem__(self, name):
        if name not in self.cache:
            content = self.get_content()
            if name not in content:
                raise KeyError
            else:
                self.cache = content

        return self.cache[name]

    def __setitem__(self, name, value):
        content = self.get_content()
        content.update({name: value})
        self.write_content(None, content)
        self.cache = content

    def write_content(self, key=None, val=None):
        raise NotImplementedError

    def get_content(self, key=None):
        raise NotImplementedError


# class FileDict(ICacheStorage):
#     """Persistent dict-like storage on a disk accessible by obj['item_name']"""
#
#     def __init__(self, filename, serializer):
#         self.filename = filename.replace(':', '_')
#         os.makedirs(os.path.dirname(self.filename), exist_ok=True)
#
#         self.cache = {}
#         self.serializer = serializer
#
#     def update(self, kwargs):
#         for key, value in kwargs.items():
#             self[key] = value
#
#     def exists(self, name):
#         try:
#             self[name]
#             return True
#
#         except KeyError:
#             return False
#
#     def __getitem__(self, name):
#         if name not in self.cache:
#             try:
#                 content = self.get_content()
#                 if name not in content:
#                     raise KeyError
#
#             except FileNotFoundError:
#                 open(self.filename, 'wb').close()
#                 raise KeyError
#
#             else:
#                 self.cache = content
#
#         return self.cache[name]
#
#     def __setitem__(self, name, value):
#         try:
#             content = self.get_content()
#         except FileNotFoundError:
#             content = {}
#
#         content.update({name: value})
#         self.write_content(None, content)
#
#         self.cache = content
#
#     def write_content(self, key=None, val=None):
#         with open(self.filename, 'wb') as f:
#             f.write(self.serializer.pack(val))
#         # self.cache = val
#
#     def get_content(self, key=None):
#         with open(self.filename, 'rb') as f:
#             content = f.read()
#             if not content:
#                 return {}
#
#         return self.serializer.unpack(content)


class Log:
    """Persistent Raft Log on a disk
    Log entries:
        {term: <term>, command: <command>}
        {term: <term>, command: <command>}
        ...
        {term: <term>, command: <command>}

    Entry index is a corresponding line number
    """

    UPDATE_CACHE_EVERY = 5

    def __init__(self, node_id, persister):
        self._persister = persister
        self.cache = self._persister.get_content()

        # All States

        """Volatile state on all servers: index of highest log entry known to be committed
        (initialized to 0, increases monotonically)"""
        self.commit_index = 0

        """Volatile state on all servers: index of highest log entry applied to state machine
        (initialized to 0, increases monotonically)"""
        self.last_applied = 0

        # Leaders

        """Volatile state on Leaders: for each server, index of the next log entry to send to that server
        (initialized to leader last log index + 1)
            {<follower>:  index, ...}
        """
        self.next_index = None

        """Volatile state on Leaders: for each server, index of highest log entry known to be replicated on server
        (initialized to 0, increases monotonically)
            {<follower>:  index, ...}
        """
        self.match_index = None

    def __getitem__(self, index):
        return self.cache[index - 1]

    def __bool__(self):
        return bool(self.cache)

    def __len__(self):
        return len(self.cache)

    def write(self, term, command):

        entry = {
            'term': term,
            'command': command
        }

        index = len(self) % self.UPDATE_CACHE_EVERY

        self._persister.write_content('append', self.cache + [entry])

        self.cache.append(entry)
        if not index:
            self.cache = self.read()

        return entry

    def read(self):
        self._persister.get_content()

    def erase_from(self, index):
        # TODO bug here
        updated = self.cache[:index - 1]
        self.cache = []

        for entry in updated:
            self.write(entry['term'], entry['command'])

    @property
    def last_log_index(self):
        """Index of last log entry staring from _one_"""
        return len(self.cache)

    @property
    def last_log_term(self):
        if self.cache:
            return self.cache[-1]['term']

        return 0


class StateMachine(object):
    """Raft Replicated State Machine — dict"""

    def __init__(self, node_id, persister: ICacheStorage):
        self._node_id = node_id
        self._persister = persister

    def apply(self, command):
        """Apply command to State Machine"""

        self._persister.update(command)


# class FileStorage(FileDict):
#     """Persistent storage
#
#     — term — latest term server has seen (initialized to 0 on first boot, increases monotonically)
#     — voted_for — candidate_id that received vote in current term (or None)
#     """
#
#     def __init__(self, log_path, node_id):
#         filename = os.path.join(log_path, '{}.storage'.format(node_id))
#         super().__init__(filename)
#
#     @property
#     def term(self):
#         return self['term']
#
#     @property
#     def voted_for(self):
#         return self['voted_for']
