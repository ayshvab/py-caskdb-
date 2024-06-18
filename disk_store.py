# pyright: strict

from pathlib import Path
from format import decode_header, decode_kv, encode_kv
import os
from typing import Tuple
from typing import BinaryIO

"""
disk_store module implements DiskStorage class which implements the KV store on the
disk

DiskStorage provides two simple operations to get and set key value pairs. Both key and
value needs to be of string type. All the data is persisted to disk. During startup,
DiskStorage loads all the existing KV pair metadata.  It will throw an error if the
file is invalid or corrupt.

Do note that if the database file is large, then the initialisation will take time
accordingly. The initialisation is also a blocking operation, till it is completed
the DB cannot be used.

Typical usage example:

    disk: DiskStorage = DiskStore(file_name="books.db")
    disk.set(key="othello", value="shakespeare")
    author: str = disk.get("othello")
    # it also supports dictionary style API too:
    disk["hamlet"] = "shakespeare"
"""


# DiskStorage is a Log-Structured Hash Table as described in the BitCask paper. We
# keep appending the data to a file, like a log. DiskStorage maintains an in-memory
# hash table called KeyDir, which keeps the row's location on the disk.
#
# The idea is simple yet brilliant:
#   - Write the record to the disk
#   - Update the internal hash table to point to that byte offset
#   - Whenever we get a read request, check the internal hash table for the address,
#       fetch that and return
#
# KeyDir does not store values, only their locations.
#
# The above approach solves a lot of problems:
#   - Writes are insanely fast since you are just appending to the file
#   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
#       storage, there could be 2-3 disk seeks
#
# However, there are drawbacks too:
#   - We need to maintain an in-memory hash table KeyDir. A database with a large
#       number of keys would require more RAM
#   - Since we need to build the KeyDir at initialisation, it will affect the startup
#       time too
#   - Deleted keys need to be purged from the file to reduce the file size
#
# Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf


class DiskStorage:
    """
    Implements the KV store on the disk

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    key_dir: dict[str, Tuple[str, int, int, int]]

    def __init__(self, file_name: str = "data.db"):
        self.timestamp = 0
        self.file_name = file_name
        self.key_dir =  {}
        if Path(file_name).is_file():
            with open(file_name, 'rb') as file:
                self.key_dir_from(file)

    def key_dir_from(self, file: BinaryIO):
        while True:
            pos = file.tell()
            header = file.read(12)
            if not header:
                break
            [timestamp, key_size, value_size] = decode_header(header)
            assert(key_size > 0)
            assert(value_size > 0)
            key_bytes = file.read(key_size)
            assert(len(key_bytes) > 0)
            key = key_bytes.decode('utf-8')
            file.seek(pos + 12 + key_size + value_size)
            self.key_dir[key] = (self.file_name, 12 + key_size + value_size, pos, timestamp)

    def next_timestamp(self):
        self.timestamp += 1
        return self.timestamp

    def set(self, key: str, value: str) -> None:
        timestamp = self.next_timestamp()
        # datum_sz is the size without header size (12 bytes), but datum includes header. WTF?
        datum_sz, datum = encode_kv(timestamp, key, value)
        with open(self.file_name, 'ab') as file:
            pos = file.tell()
            self.key_dir[key] = (self.file_name, datum_sz + 12, pos, timestamp)
            file.write(datum)

    def get(self, key: str) -> str:
        found_value = ""
        if key in self.key_dir:
            [file_name, datum_sz, pos, timestamp] = self.key_dir[key]
            assert(file_name == self.file_name)
            with open(file_name, 'rb') as file:
                file.seek(pos)
                datum = file.read(datum_sz)
                [stored_timestamp, stored_key, stored_value] = decode_kv(datum)
                assert(key == stored_key)
                assert(timestamp == stored_timestamp)
                found_value = stored_value
        return found_value

    def close(self) -> None:
        with open(self.file_name, 'rb+') as file:
            file.flush()
            os.fsync(file.fileno())

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)
