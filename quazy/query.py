from __future__ import annotations

import asyncio
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass, field as data_field
import typing
from inspect import currentframe
from types import SimpleNamespace
from collections import OrderedDict
from enum import Enum

from quazy.db import DBFactory, DBField, DBTable
from quazy.exceptions import *

if typing.TYPE_CHECKING:
    from typing import *
    from asyncpg.prepared_stmt import PreparedStatement

__all__ = ['DBQuery', 'DBScheme']


class DBQueryField:
    def __init__(self, query: DBQuery, table: Type[DBTable], path: str = None):
        self._query: DBQuery = query
        self._table: Type[DBTable] = table
        self._path: str = path or table._snake_name_

    def __getattr__(self, item):
        if item.startswith('_'):
            return super().__getattribute__(item)

        if self._path not in self._query.joins:
            self._query.joins[self._path] = DBJoin(self._table, DBJoinKind.SOURCE)

        if item in self._table.fields:
            field: DBField = self._table.fields[item]
            if field.ref:
                join_path = f'{self._path}__{field.type._snake_name_}'
                if join_path not in self._query.joins:
                    self._query.joins[join_path] = DBJoin(field.type, DBJoinKind.LEFT, f'{self._path}.{item} = {join_path}.{field.type._pk_.name}')
                return DBQueryField(self._query, field.type, join_path)
            return DBSQL(self._query, f'{self._path}.{item}')

        elif item in self._table._subtables_:
            table = self._table._subtables_[item]
            join_path = f'{self._path}__{table._snake_name_}'
            if join_path not in self._query.joins:
                self._query.joins[join_path] = DBJoin(table, DBJoinKind.LEFT,
                                                  f'{self._path}.{self._table._pk_.name} = {join_path}.{self._table._table_}')
            return DBQueryField(self._query, table, join_path)

        elif item in self._table._many_fields_:
            table = self._table._many_fields_
            join_path = f'{self._path}__{table._snake_name_}'
            if join_path not in self._query.joins:
                self._query.joins[join_path] = DBJoin(table, DBJoinKind.LEFT,
                                                  f'{self._path}.{self._table._pk_.name} = {join_path}.{self._table._table_}')
            return DBQueryField(self._query, table, join_path)

        raise QuazyFieldNameError(f'field {item} not found in {self._table.__name__}')

    def __str__(self):
        return self._path

    def __eq__(self, other) -> DBSQL:
        return typing.cast(DBSQL, getattr(self, self._table._pk_.name)) == other

    def __ne__(self, other) -> DBSQL:
        return typing.cast(DBSQL, getattr(self, self._table._pk_.name)) != other

    def __contains__(self, item) -> DBSQL:
        return typing.cast(DBSQL, getattr(self, self._table._pk_.name) in item)


class DBSQL:
    __slots__ = ['sql_text', 'query', 'aggregated']

    def __new__(cls, query: DBQuery, sql: Union[str, int, DBSQL], aggregated: bool = False):
        if isinstance(sql, DBSQL):
            return sql
        return super().__new__(cls)

    def __init__(self, query: DBQuery, sql: Union[str, int, DBSQL], aggregated: bool = False):
        self.query = query
        self.sql_text = str(sql)
        self.aggregated = aggregated

    def sql(self, sql: str) -> DBSQL:
        return DBSQL(self.query, sql, self.aggregated)

    def arg(self, value: Any) -> DBSQL:
        return self.query.arg(value, self.aggregated)

    def func1(self, op: str) -> DBSQL:
        return self.sql(f'{op}({self.sql_text})')

    def aggregate(self, op: str) -> DBSQL:
        self.aggregated = True
        return self.func1(op)

    def op(self, op: str, other: Any) -> DBSQL:
        return self.sql(f'{self.sql_text}{op}{self.query.arg(other)!r}')

    def func2(self, op: str, other: Any) -> DBSQL:
        return self.sql(f'{op}({self.sql_text}, {self.query.arg(other)!r})')

    def cast(self, type_name: str) -> DBSQL:
        return self.sql(f'{self.sql_text}::{type_name}')

    def __add__(self, other) -> DBSQL:
        return self.op('+', other)

    def __radd__(self, other) -> DBSQL:
        return self.arg(other) + self

    def __sub__(self, other) -> DBSQL:
        return self.op('-', other)

    def __rsub__(self, other) -> DBSQL:
        return self.arg(other) - self

    def __mul__(self, other) -> DBSQL:
        return self.op('*', other)

    def __rmul__(self, other) -> DBSQL:
        return self.arg(other) * self

    def __truediv__(self, other) -> DBSQL:
        return self.op('/', other)

    def __rtruediv__(self, other) -> DBSQL:
        return self.arg(other) / self

    def __mod__(self, other) -> DBSQL:
        return self.op('%', other)

    def __rmod__(self, other) -> DBSQL:
        return self.arg(other) % self

    def __pow__(self, power, modulo=None) -> DBSQL:
        return self.op('^', power)

    def __rpow__(self, other) -> DBSQL:
        return self.arg(other) ** self

    def __abs__(self) -> DBSQL:
        return self.func1('@')

    def __neg__(self) -> DBSQL:
        return self.func1('-')

    def __and__(self, other) -> DBSQL:
        return self.op(' AND ', other)

    def __rand__(self, other) -> DBSQL:
        return self.arg(other) & self

    def __or__(self, other) -> DBSQL:
        return self.op(' OR ', other)

    def __ror__(self, other) -> DBSQL:
        return self.arg(other) | self

    def __xor__(self, other) -> DBSQL:
        return self.op('#', other)

    def __rxor__(self, other) -> DBSQL:
        return self.arg(other) ^ self

    def __invert__(self) -> DBSQL:
        return self.func1('~')

    def __lshift__(self, other) -> DBSQL:
        return self.op('<<', other)

    def __rlshift__(self, other) -> DBSQL:
        return self.arg(other) << self

    def __rshift__(self, other) -> DBSQL:
        return self.op('>>', other)

    def __rrshift__(self, other) -> DBSQL:
        return self.arg(other) >> self

    def __eq__(self, other) -> DBSQL:
        return self.op('=', other)

    def __ne__(self, other) -> DBSQL:
        return self.op('<>', other)

    def __gt__(self, other) -> DBSQL:
        return self.op('>', other)

    def __ge__(self, other) -> DBSQL:
        return self.op('>=', other)

    def __lt__(self, other) -> DBSQL:
        return self.op('<', other)

    def __le__(self, other) -> DBSQL:
        return self.op('<=', other)

    def __str__(self) -> DBSQL:
        return self.cast('text')

    def __int__(self) -> DBSQL:
        return self.cast('int')

    def __float__(self) -> DBSQL:
        return self.cast('double precision')

    def __bool__(self) -> DBSQL:
        return self.cast('bool')

    def __round__(self, n=None) -> DBSQL:
        return self.func2('round', n)

    def __trunc__(self) -> DBSQL:
        return self.func1('trunc')

    def __contains__(self, item) -> DBSQL:
        return self.op(' in ', item)

    def __repr__(self):
        return self.sql_text


class DBJoinKind(Enum):
    SOURCE = "SOURCE"  # no join, base table to select
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    INNER = "INNER"
    OUTER = "OUTER"


@dataclass
class DBJoin:
    source: Union[Type[DBTable], DBQuery]
    kind: DBJoinKind
    condition: Optional[Union[str, DBSQL]] = data_field(default=None)


class DBScheme(SimpleNamespace):
    pass


if typing.TYPE_CHECKING:
    FDBSQL = Union[DBSQL, Callable[[SimpleNamespace], DBSQL]]


class DBQuery:
    queries: Dict[Hashable, DBQuery] = {}

    class SaveException(Exception):
        pass

    def __init__(self, db: DBFactory, table_class: Optional[Type[DBTable]] = None):
        self.db: DBFactory = db
        self.fields: OrderedDict[str, DBSQL] = OrderedDict()
        self.joins: OrderedDict[str, DBJoin] = OrderedDict()
        self.sort_list: List[DBSQL] = []
        self.filters: List[DBSQL] = []
        self.groups: List[DBSQL] = []
        self.group_filters: List[DBSQL] = []
        self.has_aggregates: bool = False
        self.args: OrderedDict[str, Any] = OrderedDict()
        self._arg_counter = 0
        self.prepared_statement: Optional[PreparedStatement] = None
        self._hash: Optional[Hashable] = None

        self.scheme: Union[SimpleNamespace, DBQueryField] = DBScheme()
        for table in self.db._tables:
            setattr(self.scheme, table._snake_name_, DBQueryField(self, table))

        if table_class is not None:
            self.joins[table_class._snake_name_] = DBJoin(table_class, DBJoinKind.SOURCE)
            table_space = DBQueryField(self, table_class)
            setattr(table_space, '_root', self.scheme)
            self.scheme = table_space

    def __enter__(self) -> DBQuery:
        return self

    def reuse(self):
        cf = currentframe()
        line_no = cf.f_back.f_lineno
        h = hash((__name__, line_no))
        if h in DBQuery.queries:
            raise DBQuery.SaveException
        self._hash = h

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type and issubclass(exc_type, DBQuery.SaveException):
            return True
        if self._hash:
            DBQuery.queries[self._hash] = self

    @contextmanager
    def get_scheme(self) -> SimpleNamespace:
        yield self.scheme

    @asynccontextmanager
    async def prepare_async(self):
        async with self.db.get_connection() as conn:
            sql = self.db._trans.select(self)
            self.prepared_statement = await conn.prepare(sql)
            yield
            self.prepared_statement = None

    @contextmanager
    def prepare(self):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        conn = loop.run_until_complete(self.db.get_connection())
        try:
            sql = self.db._trans.select(self)
            self.prepared_statement = loop.run_until_complete(conn.prepare_async(sql))
            yield
        finally:
            self.prepared_statement = None
            loop.run_until_complete(self.db.release_connection(conn))

    def arg(self, value: Any, aggregated: bool = False) -> DBSQL:
        if isinstance(value, DBSQL):
            return value
        if isinstance(value, str):
            return DBSQL(self, f"'{value}'")
        if value is None:
            return DBSQL(self, 'NULL')
        if isinstance(value, DBTable):
            value = getattr(value, value._pk_.name)
        if value in self.args.values():
            key = list(self.args.keys())[list(self.args.values()).index(value)]
            return DBSQL(self, f'%({key})s', aggregated)
        self._arg_counter += 1
        key = f'_arg_{self._arg_counter}'
        self.args[key] = value
        return DBSQL(self, f'%({key})s', aggregated)

    def var(self, key: str, value: Optional[Any] = None) -> DBSQL:
        self.args[key] = value
        return DBSQL(self, f'%({key})')

    def _eval_field(self, field: FDBSQL) -> DBSQL:
        return field(self.scheme) if callable(field) else field

    def select(self, **fields) -> DBQuery:
        for field_name, field_value in fields.items():
            self.fields[field_name] = self._eval_field(field_value)
        return self

    def select_all(self) -> DBQuery:
        self.fields['*'] = DBSQL(self, '*')
        return self

    def sort_by(self, *fields) -> DBQuery:
        for field in fields:
            self.sort_list.append(DBSQL(self, field))
        return self

    def filter(self, expression: FDBSQL) -> DBQuery:
        self.filters.append(self._eval_field(expression))
        return self

    def group_filter(self, expression: FDBSQL) -> DBQuery:
        self.group_filters.append(self._eval_field(expression))
        return self

    def group_by(self, *fields: FDBSQL) -> DBQuery:
        for field in fields:
            self.groups.append(DBSQL(self, self._eval_field(field)))
        return self

    def __getitem__(self, item: str):
        return getattr(self.scheme, item)

    def sum(self, expr: DBSQL):
        self.has_aggregates = True
        return expr.aggregate('sum')

    def count(self, expr: DBSQL = None):
        if expr is None:
            expr = DBSQL(self, '*')
        self.has_aggregates = True
        return expr.aggregate('count')

    def avg(self, expr: DBSQL):
        self.has_aggregates = True
        return expr.aggregate('avg')

    def min(self, expr: DBSQL):
        self.has_aggregates = True
        return expr.aggregate('min')

    def max(self, expr: DBSQL):
        self.has_aggregates = True
        return expr.aggregate('max')

    def fetch(self):
        return self.db.select(self)

    def fetchone(self):
        return self.fetch().fetchone()

    def fetch_aggregate(self, function: str):
        if len(self.fields) > 0:
            raise QuazyError(f'Use `fetch_{function}` as single field')
        self.fields['result'] = DBSQL(self, '*').aggregate(function)
        return self.fetch().fetchone().result

    def fetch_count(self):
        return self.fetch_aggregate('count')

    def fetch_max(self):
        return self.fetch_aggregate('max')

    def fetch_min(self):
        return self.fetch_aggregate('min')

    def fetch_avg(self):
        return self.fetch_aggregate('avg')

