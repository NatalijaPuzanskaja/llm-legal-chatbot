import logging
import psycopg2
import psycopg2.extras
from typing import Iterator, Iterable, ContextManager, List, Dict, Any, Tuple, Optional
from typing_extensions import Protocol, Type
from types import TracebackType
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from .iteration import iterable

ColumnDescription = Tuple[str, Any, Any, Any, Any, Any, Any]

logger = logging.getLogger('sql_engine')


class DbCursor(Protocol):
    @property
    def rowcount(self) -> int:
        ...

    @property
    def description(self) -> Optional[List[ColumnDescription]]:
        ...

    def execute(self, sql: str, parameters: Dict[str, Any] = None) -> None:
        ...

    def execute_many(self, sql: str, parameters: Iterable[Dict[str, Any]]) -> None:
        ...

    def fetchone(self) -> Optional[tuple]:
        ...

    def fetchmany(self, size: int = 1) -> List[tuple]:
        ...

    def close(self) -> None:
        ...


class PostgresCursor(DbCursor):
    def __init__(self, underlying: DbCursor) -> None:
        self.underlying = underlying

    @property
    def rowcount(self) -> int:
        return self.underlying.rowcount

    @property
    def description(self) -> Optional[List[ColumnDescription]]:
        return self.underlying.description

    def execute(self, sql: str, parameters: Dict[str, Any] = None) -> None:
        logger.debug('Execute:\n%s\nparams=%s', sql, parameters)
        self.underlying.execute(sql, parameters)

    def execute_many(self, sql: str, parameters: Iterable[Dict[str, Any]]) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Execute batch with %s params:\n%s', len(list(parameters)), sql)
        psycopg2.extras.execute_batch(self.underlying, sql, parameters)

    def fetchone(self) -> Optional[tuple]:
        return self.underlying.fetchone()

    def fetchmany(self, size: int = 1) -> List[tuple]:
        return self.underlying.fetchmany(size)

    def close(self) -> None:
        self.underlying.close()


class DbConnection(Protocol):
    @property
    def closed(self) -> int:
        ...

    @property
    def autocommit(self) -> bool:
        ...

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        ...

    def cursor(self) -> DbCursor:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...


class PostgresConnection(DbConnection):
    underlying: DbConnection

    def __init__(self, underlying: DbConnection) -> None:
        self.underlying = underlying

    @property
    def closed(self) -> int:
        return self.underlying.closed

    @property
    def autocommit(self) -> bool:
        return self.underlying.autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self.underlying.autocommit = value

    def cursor(self) -> DbCursor:
        return PostgresCursor(self.underlying.cursor())

    def commit(self) -> None:
        self.underlying.commit()

    def rollback(self) -> None:
        self.underlying.rollback()


class DataSource(Protocol):
    def get_connection(self) -> ContextManager[DbConnection]:
        ...

    def close(self) -> None:
        ...


class ConnectionPool(Protocol):
    def getconn(self) -> DbConnection:
        ...

    def putconn(self, connection: DbConnection) -> None:
        ...

    def closeall(self) -> None:
        ...


class DbTransaction:
    data_source: DataSource
    connection: DbConnection
    prev_autocommit: bool
    should_rollback = False
    connection_manager: ContextManager[DbConnection]
    live_cursors: List[DbCursor]

    def __init__(self, data_source: DataSource) -> None:
        self.data_source = data_source
        self.live_cursors = []

    def __enter__(self) -> 'DbTransaction':
        self.connection_manager = self.data_source.get_connection()
        self.connection = self.connection_manager.__enter__()
        self.prev_autocommit = self.connection.autocommit
        self.connection.autocommit = False
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> Optional[bool]:
        if exc_type:
            self.should_rollback = True

        for cursor in self.live_cursors:
            cursor.close()

        if self.should_rollback:
            if not self.connection.closed:
                self.connection.rollback()
        else:
            self.connection.commit()

        if not self.connection.closed:
            self.connection.autocommit = self.prev_autocommit
        return self.connection_manager.__exit__(exc_type, exc_value, traceback)

    @iterable
    def execute_query(self, sql: str, **kwargs) -> Iterable[Dict[str, Any]]:
        cursor = self.connection.cursor()
        try:
            self.live_cursors.append(cursor)
            cursor.execute(sql, kwargs)
            if cursor.description is not None:
                col_names = [i[0] for i in cursor.description]
            else:
                col_names = []

            rows = cursor.fetchmany(1000)
            while len(rows) > 0:
                for row in rows:
                    record: Dict[str, Any] = {}
                    for i, val in enumerate(row):
                        record[col_names[i]] = val
                    yield record
                rows = cursor.fetchmany(1000)
        finally:
            cursor.close()
            self.live_cursors.remove(cursor)

    def execute_scalar(self, sql: str, **kwargs) -> Any:
        cursor = self.connection.cursor()
        try:
            self.live_cursors.append(cursor)
            cursor.execute(sql, kwargs)
            row = cursor.fetchone()
            if row is None:
                return None
            return row[0]
        finally:
            cursor.close()
            self.live_cursors.remove(cursor)

    def execute_statement(self, sql: str, **kwargs) -> int:
        cursor = self.connection.cursor()
        try:
            self.live_cursors.append(cursor)
            cursor.execute(sql, kwargs)
            return cursor.rowcount
        finally:
            cursor.close()
            self.live_cursors.remove(cursor)

    def execute_batch(self, sql, parameters: Iterable[Dict[str, Any]]) -> None:
        cursor = self.connection.cursor()
        try:
            self.live_cursors.append(cursor)
            cursor.execute_many(sql, parameters)
        finally:
            cursor.close()
            self.live_cursors.remove(cursor)

    def rollback(self) -> None:
        self.should_rollback = True


class SqlEngine:
    data_source: DataSource

    def __init__(self, data_source: DataSource) -> None:
        self.data_source = data_source

    def begin_transaction(self) -> ContextManager[DbTransaction]:
        return DbTransaction(self.data_source)


class PostgresDataSource(DataSource):
    pool: ConnectionPool

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    @classmethod
    def from_credentials(cls, *, host: str, database: str, user: str, password: str) -> 'PostgresDataSource':
        return cls(
            ThreadedConnectionPool(
                1,
                8,
                host=host,
                database=database,
                user=user,
                password=password,
            )
        )

    @contextmanager
    def get_connection(self) -> Iterator[DbConnection]:
        try:
            connection = self.pool.getconn()
            yield PostgresConnection(connection)
        finally:
            if connection:
                self.pool.putconn(connection)

    def close(self) -> None:
        self.pool.closeall()


class QueryBuilder:
    sql: str
    param_index: int
    conditions: List[str]
    parameters: Dict[str, Any]
    orders: List[str]
    query_limit: Optional[int]
    query_offset: Optional[int]

    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.param_index = 0
        self.conditions = []
        self.parameters = {}
        self.orders = []
        self.query_limit = None
        self.query_offset = None

    def where(self, conditition: str, *params: Any) -> 'QueryBuilder':
        obj = self.copy()
        param_names: List[str] = []
        for param in params:
            param_name = f'param{obj.param_index}'
            obj.parameters[param_name] = param
            param_names.append(f'%({param_name})s')
            obj.param_index += 1

        obj.conditions.append(conditition.format(*param_names))
        return obj

    def order_by(self, *args: str) -> 'QueryBuilder':
        obj = self.copy()
        obj.orders.extend(args)
        return obj

    def limit(self, value: int) -> 'QueryBuilder':
        obj = self.copy()
        obj.query_limit = value
        return obj

    def offset(self, value: int) -> 'QueryBuilder':
        obj = self.copy()
        obj.query_offset = value
        return obj

    def copy(self) -> 'QueryBuilder':
        obj = QueryBuilder(self.sql)
        obj.param_index = self.param_index
        obj.conditions = self.conditions[:]
        obj.parameters = dict(self.parameters)
        obj.orders = self.orders[:]
        obj.query_limit = self.query_limit
        obj.query_offset = self.query_offset
        return obj

    def build(self) -> dict:
        sql_parts = [self.sql]
        if len(self.conditions) > 0:
            sql_parts.append('\nWHERE')
            for i, condition in enumerate(self.conditions):
                if i > 0:
                    sql_parts.append('\n AND ')
                else:
                    sql_parts.append(' ')

                sql_parts.append('(')
                sql_parts.append(condition)
                sql_parts.append(')')

        if len(self.orders) > 0:
            sql_parts.append('\nORDER BY ')
            sql_parts.append(', '.join(self.orders))

        if self.query_limit is not None:
            sql_parts.append('\nLIMIT ')
            sql_parts.append(str(self.query_limit))

        if self.query_offset is not None:
            sql_parts.append('\nOFFSET ')
            sql_parts.append(str(self.query_offset))

        result = {
            'sql': ''.join(sql_parts),
        }
        result.update(self.parameters)
        return result
