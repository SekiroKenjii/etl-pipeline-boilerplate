"""Uniform random sampling of streams (reservoir sampling)."""

from __future__ import annotations

import random


def reservoir_offer[T](
    sample: list[T], item: T, seen: int, capacity: int, rng: random.Random
) -> None:
    """Offers one item of a stream to a reservoir (Algorithm R).

    Every item of the stream ends up in `sample` with the same probability,
    using O(capacity) memory.

    Args:
        sample: The reservoir, updated in place.
        item: The item offered.
        seen: The position of `item` in the stream, from 1.
        capacity: The size of the reservoir; 0 keeps nothing.
        rng: The random generator.
    """
    if capacity <= 0:
        return

    if len(sample) < capacity:
        sample.append(item)
        return

    slot = rng.randrange(seen)
    if slot < capacity:
        sample[slot] = item


def merge_reservoirs[T](
    first: list[T],
    first_seen: int,
    second: list[T],
    second_seen: int,
    capacity: int,
    rng: random.Random,
) -> list[T]:
    """Merges uniform samples of two disjoint streams into one.

    Each pick comes from a stream with a probability proportional to the
    number of its items not picked yet, which is what drawing without
    replacement from the combined stream would do.

    Args:
        first: A uniform sample of the first stream.
        first_seen: The number of items in the first stream.
        second: A uniform sample of the second stream.
        second_seen: The number of items in the second stream.
        capacity: The size of the merged sample.
        rng: The random generator.

    Returns:
        A uniform sample of the combined stream, of at most `capacity` items.
    """
    left, right = list(first), list(second)
    rng.shuffle(left)
    rng.shuffle(right)
    left_remaining, right_remaining = first_seen, second_seen
    merged: list[T] = []

    while len(merged) < capacity and (left or right):
        total = left_remaining + right_remaining
        take_left = bool(left) and (
            not right or rng.random() < left_remaining / total
        )

        if take_left:
            merged.append(left.pop())
            left_remaining -= 1
        else:
            merged.append(right.pop())
            right_remaining -= 1

    return merged
