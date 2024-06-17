from struct import pack, unpack

"""
format module provides encode/decode functions for serialisation and deserialisation
operations

format module is generic, does not have any disk or memory specific code.

The disk storage deals with bytes; you cannot just store a string or object without
converting it to bytes. The programming languages provide abstractions where you don't
have to think about all this when storing things in memory (i.e. RAM). Consider the
following example where you are storing stuff in a hash table:

    books = {}
    books["hamlet"] = "shakespeare"
    books["anna karenina"] = "tolstoy"

In the above, the language deals with all the complexities:

    - allocating space on the RAM so that it can store data of `books`
    - whenever you add data to `books`, convert that to bytes and keep it in the memory
    - whenever the size of `books` increases, move that to somewhere in the RAM so that
      we can add new items

Unfortunately, when it comes to disks, we have to do all this by ourselves, write
code which can allocate space, convert objects to/from bytes and many other operations.

format module provides two functions which help us with serialisation of data.

    encode_kv - takes the key value pair and encodes them into bytes
    decode_kv - takes a bunch of bytes and decodes them into key value pairs

**workshop note**

For the workshop, the functions will have the following signature:

    def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]
    def decode_kv(data: bytes) -> tuple[int, str, str]
"""


def encode_header(timestamp: int, key_size: int, value_size: int) -> bytes:
    return pack("<III", timestamp, key_size, value_size)


def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]:
    key_bytes = key.encode('utf-8')
    value_bytes = value.encode('utf-8')
    key_size = len(key_bytes)
    value_size = len(value_bytes)
    header = encode_header(timestamp, key_size, value_size)
    datum = header + key_bytes + value_bytes
    # Header size is 12 bytes (3 * 4 bytes for each integer)
    return len(datum) - 12, datum

def decode_kv(data: bytes) -> tuple[int, str, str]:
    timestamp, key_size, value_size = unpack("<III", data[:12])
    key, value = unpack(f"<{key_size}s{value_size}s", data[12:])
    return timestamp, key.decode('utf-8'), value.decode('utf-8')

def decode_header(data: bytes) -> tuple[int, int, int]:
    return unpack("<III", data)

