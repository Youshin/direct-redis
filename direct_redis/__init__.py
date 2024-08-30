"""
DirectRedis package for enhanced Redis interactions with automatic serialization.

This package provides the DirectRedis class, which extends the functionality of
the standard Redis client to automatically handle serialization and deserialization
of Python objects. It allows for easier storage and retrieval of complex data types
in Redis, including custom Python objects, without manual encoding and decoding.

The main class provided by this package is:
    - DirectRedis: An extended Redis client with automatic serialization capabilities.

Usage:
    from direct_redis import DirectRedis

    # Create a DirectRedis instance
    dr = DirectRedis(host='localhost', port=6379, db=0)

    # Use it like a regular Redis client, but with automatic serialization
    dr.set('key', {'complex': 'data'})
    data = dr.get('key')  # Returns the deserialized Python dictionary

This package simplifies Redis operations when working with complex Python data structures,
making it easier to integrate Redis into Python applications that deal with non-string data.
"""

from .direct_redis import DirectRedis
