import hashlib

from typing import Sequence


def hash_sequence(items: Sequence) -> str:
    hasher = hashlib.sha256()
    for item in sorted(items):
        hasher.update(item.encode())

    return hasher.hexdigest()
