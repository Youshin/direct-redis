"""
Module containing utility functions for the direct_redis package.

This module provides functions for data type conversion, serialization,
and deserialization to be used with Redis.
"""

import struct
import pickle
import numpy as np

__all__ = ["convert_set_type", "convert_set_mapping_dic", "convert_get_type"]


def isinstances(obj, classinfos: list):
    """
    Check if an object is an instance of any class in the given list.

    :param obj: The object to check
    :param classinfos: List of classes to check against
    :return: True if the object is an instance of any class in the list, False otherwise
    """
    return any(isinstance(obj, c) for c in classinfos)


def convert_set_type(value):
    """
    Convert a value to a format suitable for storing in Redis.

    :param value: The value to convert
    :return: Converted value (string or serialized object)
    """
    if isinstance(value, str):
        return value
    return pickle.dumps(value)


def convert_set_mapping_dic(dic):
    """
    Convert all values in a dictionary to a format suitable for storing in Redis.

    :param dic: The dictionary to convert
    :return: Converted dictionary
    """
    return {k: convert_set_type(v) for k, v in dic.items()}


def convert_get_type(encoded, pickle_first):
    """
    Convert an encoded value retrieved from Redis back to its original type.

    :param encoded: The encoded value
    :param pickle_first: Whether to attempt pickle deserialization first
    :return: Converted value
    """
    if encoded is None:
        return None

    if pickle_first:
        try:
            return pickle.loads(encoded)
        except pickle.PickleError:
            pass

    try:
        return encoded.decode("utf-8")
    except UnicodeDecodeError:
        if not pickle_first:
            try:
                return pickle.loads(encoded)
            except pickle.PickleError:
                pass

    return encoded


def serialize_np(np_array):
    """
    Serialize a NumPy array.

    :param np_array: The NumPy array to serialize
    :return: Serialized byte string
    :raises ValueError: If the array has unsupported dimensions
    """
    shape = np_array.shape
    if len(shape) == 1:
        d1, d2, d3 = shape[0], 0, 0
    elif len(shape) == 2:
        d1, d2, d3 = shape[0], shape[1], 0
    elif len(shape) == 3:
        d1, d2, d3 = shape
    else:
        raise ValueError("Redis can only store 1D, 2D, and 3D NumPy arrays.")

    dtype = np_array.dtype.num
    packed_data = struct.pack(">IIII", dtype, d1, d2, d3)
    return packed_data + np_array.tobytes()


def unserialize_np(encoded):
    """
    Deserialize a serialized NumPy array.

    :param encoded: The serialized NumPy array
    :return: Deserialized NumPy array
    """
    dtype, d1, d2, d3 = struct.unpack(">IIII", encoded[:16])
    np_array = np.frombuffer(encoded, offset=16, dtype=np.typeDict[dtype])
    if d3 != 0:
        return np_array.reshape(d1, d2, d3)
    if d2 != 0:
        return np_array.reshape(d1, d2)
    return np_array
