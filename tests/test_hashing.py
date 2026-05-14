import hashlib

from satsignal_blob.hashing import hash_object


def test_hash_small_file(memfs):
    payload = b"hello satsignal\n"
    expected = hashlib.sha256(payload).hexdigest()
    memfs.pipe("/bucket/contract.pdf", payload)

    sha, size = hash_object(memfs, "/bucket/contract.pdf")

    assert sha == expected
    assert size == len(payload)


def test_hash_streams_chunks(memfs):
    # Pick a payload larger than the chunk to force multiple reads.
    payload = (b"abc" * (1 << 19))  # 1.5 MiB
    expected = hashlib.sha256(payload).hexdigest()
    memfs.pipe("/bucket/big.bin", payload)

    sha, size = hash_object(memfs, "/bucket/big.bin", chunk_size=64 * 1024)

    assert sha == expected
    assert size == len(payload)


def test_hash_empty_file(memfs):
    memfs.pipe("/bucket/empty.txt", b"")
    sha, size = hash_object(memfs, "/bucket/empty.txt")
    assert sha == hashlib.sha256(b"").hexdigest()
    assert size == 0
