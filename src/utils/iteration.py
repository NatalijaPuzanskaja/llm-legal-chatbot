from typing import TypeVar, Any, cast, Iterable, List, Dict, Callable
from functools import wraps


T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


def iterable(f: T) -> T:
    @wraps(cast(Any, f))
    def wrapper(*args, **kwargs):
        class IterableWrapper:
            def __iter__(self):
                return f(*args, **kwargs)

        return IterableWrapper()
    return cast(T, wrapper)


def group_by(values: Iterable[T], key_selector: Callable[[T], U]) -> Dict[U, List[T]]:
    result: Dict[U, List[T]] = {}
    for value in values:
        key = key_selector(value)
        group = result.get(key, [])
        group.append(value)
        result[key] = group
    return result


def index_by(
        values: Iterable[T],
        key_selector: Callable[[T], U],
        value_selector: Callable[[T], V],
) -> Dict[U, V]:
    result: Dict[U, V] = {}
    for value in values:
        key = key_selector(value)
        result[key] = value_selector(value)
    return result


@iterable
def batch_by(values: Iterable[T], batch_size: int) -> Iterable[List[T]]:
    batch: List[T] = []
    for val in values:
        batch.append(val)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch
