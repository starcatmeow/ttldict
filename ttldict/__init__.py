__all__ = ['TTLDict']

from collections import deque
from typing import TypeVar, Generic, Optional
from threading import RLock
import time
K = TypeVar('K')
V = TypeVar('V')
class TTLDict(Generic[K, V]):
    ttl: float
    d: dict[K, tuple[float, V]]
    expiring_queue: deque[tuple[float, K, Optional[function]]]
    lock: RLock
    def __init__(self, ttl: float, *args, **kwargs):
        self.ttl = ttl
        self.d = dict(*args, **kwargs)
        self.expiring_queue = deque()
        self.lock = RLock()
    def purge(self):
        with self.lock:
            cur_time = time.time()
            while len(self.expiring_queue) > 0 and self.expiring_queue[0][0] < cur_time:
                _, key, cb = self.expiring_queue.popleft()
                if key in self.d:
                    exp, _ = self.d[key]
                    if exp < cur_time:
                        del self.d[key]
                        if cb is not None:
                            cb()
    def __contains__(self, key: K) -> bool:
        with self.lock:
            self.purge()
            return key in self.d
    def __delitem__(self, key: K):
        with self.lock:
            self.purge()
            del self.d[key]
    def __getitem__(self, key: K) -> V:
        with self.lock:
            self.purge()
            return self.d[key][1]
    def __len__(self) -> int:
        with self.lock:
            self.purge()
            return len(self.d)
    def __setitem__(self, key: K, value: V):
        with self.lock:
            self.purge()
            exp = time.time() + self.ttl
            self.d[key] = exp, value
            self.expiring_queue.append((exp, key, None))
    def clear(self):
        with self.lock:
            self.d.clear()
            self.expiring_queue.clear()
    def get(self, key: K, default: Optional[V] = None) -> V:
        with self.lock:
            self.purge()
            return self.d.get(key, (0, default))[1]
    def items(self) -> list[tuple[K, V]]:
        with self.lock:
            self.purge()
            return [(k,v[1]) for k, v in self.d.items()]
    def keys(self) -> list[K]:
        with self.lock:
            self.purge()
            return self.d.keys()
    def values(self) -> list[V]:
        with self.lock:
            self.purge()
            return [v[1] for v in self.d.values()]
    def pop(self, *args) -> V:
        with self.lock:
            self.purge()
            return self.d.pop(*args)[1]
    def popitem(self) -> tuple[K, V]:
        with self.lock:
            self.purge()
            k, v = self.d.popitem()
            return k, v[1]
    def setdefault(self, key, default: Optional[V] = None) -> Optional[V]:
        with self.lock:
            self.purge()
            if key in self.d:
                return self.d[key][1]
            self.__setitem__(key, default)
            return default
    def set(self, key, value, expire_callback: Optional[function] = None):
        with self.lock:
            self.purge()
            exp = time.time() + self.ttl
            self.d[key] = exp, value
            self.expiring_queue.append((exp, key, expire_callback))
