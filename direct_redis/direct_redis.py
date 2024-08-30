"""
DirectRedis module provides an extended Redis client with enhanced serialization capabilities.

This module extends the Redis class to provide automatic serialization and deserialization
of Python objects when interacting with Redis, allowing for easier storage and retrieval
of complex data types.
"""

from typing import Any, Dict, List, Optional, Union

try:
    from redis import Redis
except ImportError:
    raise ImportError(
        "Redis is not installed. Please install it using pip install redis"
    )
from direct_redis.functions import (
    convert_set_type,
    convert_set_mapping_dic,
    convert_get_type,
)


class DirectRedis(Redis):
    """
    DirectRedis extends the Redis class to provide automatic serialization and deserialization.

    This class overrides several Redis methods to automatically handle conversion between
    Python objects and Redis-compatible formats, allowing for easier storage and retrieval
    of complex data types.
    """

    def keys(self, pattern: str = "*", **kwargs: Any) -> List[str]:
        """Get all keys matching pattern."""
        encoded = super().keys(pattern, **kwargs)
        return [convert_get_type(key, pickle_first=False) for key in encoded or []]

    def randomkey(self, **kwargs: Any) -> Optional[str]:
        """Return a random key from the keyspace."""
        encoded = super().randomkey(**kwargs)
        return convert_get_type(encoded, pickle_first=False)

    def type(self, name: str) -> Optional[str]:
        """Determine the type stored at key."""
        encoded = super().type(name)
        return convert_get_type(encoded, pickle_first=False)

    def set(
        self,
        name: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set the value at key ``name`` to ``value``"""
        return super().set(name, convert_set_type(value), ex, px, nx, xx)

    def get(self, name: str, pickle_first: bool = False) -> Any:
        """Return the value at key ``name``, or None if the key doesn't exist"""
        encoded = super().get(name)
        return convert_get_type(encoded, pickle_first)

    def mset(self, mapping: Dict[str, Any]) -> bool:
        """Set key/values based on a mapping."""
        if not isinstance(mapping, dict):
            raise ValueError("mapping must be a python dictionary")
        mapping = convert_set_mapping_dic(mapping)
        return super().mset(mapping)

    def mget(self, *names: str, pickle_first: bool = False) -> List[Any]:
        """Returns a list of values ordered identically to ``names``"""
        encoded = super().mget(names)
        return [convert_get_type(value, pickle_first) for value in encoded or []]

    def hkeys(self, name: str) -> List[str]:
        """Return the list of keys within hash ``name``"""
        encoded = super().hkeys(name)
        return [convert_get_type(key, pickle_first=False) for key in encoded or []]

    def hset(
        self,
        name: str,
        key: Optional[str] = None,
        value: Optional[Any] = None,
        mapping: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Set ``key`` to ``value`` within hash ``name``"""
        if mapping:
            mapping = convert_set_mapping_dic(mapping)
            return super().hset(name, mapping=mapping)
        return super().hset(name, key, convert_set_type(value))

    def hmset(self, name: str, mapping: Dict[str, Any]) -> bool:
        """Set key to value within hash ``name``
        for each corresponding key and value from the ``mapping`` dict."""
        if not isinstance(mapping, dict):
            raise ValueError("mapping must be a python dictionary")
        mapping = convert_set_mapping_dic(mapping)
        return super().hmset(name, mapping)

    def hget(self, name: str, key: str, pickle_first: bool = False) -> Any:
        """Return the value of ``key`` within the hash ``name``"""
        encoded = super().hget(name, key)
        return convert_get_type(encoded, pickle_first)

    def hmget(
        self, name: str, keys: List[str], pickle_first: bool = False
    ) -> List[Any]:
        """Returns a list of values ordered identically to ``keys``"""
        encoded = super().hmget(name, keys)
        return [convert_get_type(value, pickle_first) for value in encoded or []]

    def hvals(self, name: str, pickle_first: bool = False) -> List[Any]:
        """Return the list of values within hash ``name``"""
        encoded = super().hvals(name)
        return [convert_get_type(value, pickle_first) for value in encoded or []]

    def hgetall(self, name: str, pickle_first: bool = False) -> Dict[str, Any]:
        """Return a Python dict of the hash's name/value pairs"""
        encoded = super().hgetall(name)
        return {
            k.decode("utf-8"): convert_get_type(v, pickle_first)
            for k, v in encoded.items()
        }

    def sadd(self, name: str, *values: Any) -> int:
        """Add ``value(s)`` to set ``name``"""
        encoded = [convert_set_type(value) for value in values]
        return super().sadd(name, *encoded)

    def srem(self, name: str, *values: Any) -> int:
        """Remove ``values`` from set ``name``"""
        encoded = [convert_set_type(value) for value in values]
        return super().srem(name, *encoded)

    def sismember(self, name: str, value: Any) -> bool:
        """Return a boolean indicating if ``value`` is a member of set ``name``"""
        encoded = convert_set_type(value)
        return super().sismember(name, encoded)

    def smembers(self, name: str, pickle_first: bool = False) -> set:
        """Return all members of the set ``name``"""
        encoded = super().smembers(name)
        return {convert_get_type(value, pickle_first) for value in encoded or []}

    def spop(
        self, name: str, count: Optional[int] = None, pickle_first: bool = False
    ) -> Union[Any, List[Any]]:
        """Remove and return a random member of set ``name``"""
        encoded = super().spop(name, count)
        if isinstance(encoded, list):
            return [convert_get_type(value, pickle_first) for value in encoded]
        return convert_get_type(encoded, pickle_first)

    def srandmember(
        self, name: str, number: Optional[int] = None, pickle_first: bool = False
    ) -> Union[Any, List[Any]]:
        """Return a random member of set ``name``"""
        encoded = super().srandmember(name, number=number)
        if isinstance(encoded, list):
            return [convert_get_type(value, pickle_first) for value in encoded or []]
        return convert_get_type(encoded, pickle_first)

    def sdiff(self, keys: Union[str, List[str]], *args: str) -> set:
        """Return the difference of sets specified by ``keys``"""
        encoded = super().sdiff(keys, *args)
        return {convert_get_type(value, pickle_first=False) for value in encoded or []}

    def lpush(self, name: str, *values: Any) -> int:
        """Push ``values`` onto the head of the list ``name``"""
        encoded = [convert_set_type(value) for value in values]
        return super().lpush(name, *encoded)

    def lpushx(self, name: str, value: Any) -> int:
        """Push ``value`` onto the head of the list ``name`` if ``name`` exists"""
        return super().lpushx(name, convert_set_type(value))

    def rpushx(self, name: str, value: Any) -> int:
        """Push ``value`` onto the tail of the list ``name`` if ``name`` exists"""
        return super().rpushx(name, convert_set_type(value))

    def rpush(self, name: str, *values: Any) -> int:
        """Push ``values`` onto the tail of the list ``name``"""
        encoded = [convert_set_type(value) for value in values]
        return super().rpush(name, *encoded)

    def lpop(
        self, name: str, count: Optional[int] = None, pickle_first: bool = False
    ) -> Union[Any, List[Any]]:
        """Remove and return the first item of the list ``name``"""
        encoded = super().lpop(name, count)
        if isinstance(encoded, list):
            return [convert_get_type(value, pickle_first) for value in encoded]
        return convert_get_type(encoded, pickle_first)

    def rpop(
        self, name: str, count: Optional[int] = None, pickle_first: bool = False
    ) -> Union[Any, List[Any]]:
        """Remove and return the last item of the list ``name``"""
        encoded = super().rpop(name, count)
        if isinstance(encoded, list):
            return [convert_get_type(value, pickle_first) for value in encoded]
        return convert_get_type(encoded, pickle_first)

    def lindex(self, name: str, index: int, pickle_first: bool = False) -> Any:
        """Return the item from list ``name`` at position ``index``"""
        encoded = super().lindex(name, index)
        return convert_get_type(encoded, pickle_first)

    def lrange(
        self, name: str, start: int = 0, end: int = -1, pickle_first: bool = False
    ) -> List[Any]:
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        encoded = super().lrange(name, start, end)
        return [convert_get_type(value, pickle_first) for value in encoded or []]
