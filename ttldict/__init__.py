__all__ = ['TTLDict']

from collections import deque
from typing import TypeVar, Generic, Optional, Callable, List, Tuple, Dict, Any, overload
from threading import RLock
import time


K = TypeVar('K')
V = TypeVar('V')


class TTLDict(Generic[K, V]):
    ttl: float
    d: Dict[K, Tuple[float, V]]
    expiring_queue: deque[tuple[float, K, Optional[Callable[[K, V], Any]]]]
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
                    exp, val = self.d[key]
                    if exp < cur_time:
                        del self.d[key]
                        if cb is not None:
                            cb(key, val)

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

    @overload
    def get(self, key: K) -> V | None: ...

    @overload
    def get(self, key: K, default: V) -> V: ...

    def get(self, key: K, default: Optional[V] = None) -> V | None:
        with self.lock:
            self.purge()
            return self.d.get(key, (0, default))[1]

    def items(self) -> List[Tuple[K, V]]:
        with self.lock:
            self.purge()
            return [(k, v[1]) for k, v in self.d.items()]

    def keys(self) -> List[K]:
        with self.lock:
            self.purge()
            return list(self.d.keys())

    def values(self) -> List[V]:
        with self.lock:
            self.purge()
            return [v[1] for v in self.d.values()]

    def pop(self, *args) -> V:
        with self.lock:
            self.purge()
            return self.d.pop(*args)[1]

    def popitem(self) -> Tuple[K, V]:
        with self.lock:
            self.purge()
            k, v = self.d.popitem()
            return k, v[1]

    def setdefault(self, key, default: V) -> V:
        with self.lock:
            self.purge()
            if key in self.d:
                return self.d[key][1]
            self.__setitem__(key, default)
            return default

    def set(self, key, value, expire_callback: Optional[Callable[[K, V], Any]] = None):
        with self.lock:
            self.purge()
            exp = time.time() + self.ttl
            self.d[key] = exp, value
            self.expiring_queue.append((exp, key, expire_callback))
