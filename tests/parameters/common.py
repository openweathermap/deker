import random

from string import ascii_letters, digits, punctuation
from typing import Union


INTEG = random.randint(1, 25)


def random_string() -> str:
    """Get random string with length from 1 to 20."""
    strings = (
        (digits + ascii_letters + punctuation).replace("/", "").replace("\\", "").replace("$", "")
    )
    return "".join((random.choice(strings) for _ in range(random.randint(5, 20))))


def _random_positive_int() -> int:
    """Get random positive integer from 1 to 6."""
    return random.randint(1, 6)


def _random_negative_int() -> int:
    """Get random negative integer from -6 to -1."""
    return random.randint(-6, -1)


def _random_positive_float() -> float:
    """Get random positive float from 1 to 6."""
    return random.uniform(1, 6)


def _random_negative_float() -> float:
    """Get random negative float from -6 to -1."""
    return random.uniform(-6, -1)


def random_step() -> Union[int, float]:
    """Get random step value from -6 to 6."""
    return random.choice(
        [
            _random_negative_int(),
            _random_positive_int(),
        ]
    )
