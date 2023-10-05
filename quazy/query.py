from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import typing
from inspect import currentframe
from types import SimpleNamespace
from collections import OrderedDict
from enum import Enum
import copy

from quazy.db import DBFactory, DBField, DBTable, T
from quazy.exceptions import *

if typing.TYPE_CHECKING:
    from typing import *


__all__ = ['DBQuery', 'DBScheme', 'DBQueryField']


class DBQueryField:
    def __init__(self, query: DBQuery, table: type[DBTable], path: str = None, field: DBField = None):
        self._query: DBQuery = query
        self._table: type[DBTable] = table
        self._path: str = path or table.DB.snake_name
        self._field: DBField = field

    def __getattr__(self, item):
        if item.startswith('_'):
            return super().__getattribute__(item)

        if not self._field:
            if self._path not in self._query.joins:
                self._query.joins[self._path] = DBJoin(self._table, DBJoinKind.SOURCE)
        else:
            join_path = f'{self._table.DB.snake_name}__{self._field.name}'
            if join_path not in self._query.joins:
                self._query.joins[join_path] = DBJoin(self._field.type, DBJoinKind.LEFT,
                                                      f'{self._path} = {join_path}.{self._field.type.DB.pk.name}')
            return getattr(DBQueryField(self._query, self._field.type, join_path), item)

        DB = self._table.DB
        if item in DB.fields:
            field: DBField = DB.fields[item]
            if not field.prop:
                field_path = f'{self._path}.{item}'
            else:
                field_path = f'{self._path}.{DB.body.name}'
                field_path = self._query.db._trans.deserialize(field, f"{field_path}->>'{item}'")

            if field.ref:
                return DBQueryField(self._query, self._table, field_path, field)
            return DBSQL(self._query, f'{field_path}')

        elif table := DB.subtables.get(item) or DB.many_fields.get(item):
            join_path = f'{self._path}__{item}'
            if join_path not in self._query.joins:
                self._query.joins[join_path] = DBJoin(table, DBJoinKind.LEFT,
                                                  f'{self._path}.{DB.pk.name} = {join_path}.{DB.table}')
            return DBQueryField(self._query, table, join_path)

        raise QuazyFieldNameError(f'field `{item}` is not found in `{DB.table}`')

    def __str__(self):
        return self._path

    def __eq__(self, other) -> DBSQL:
        return DBSQL(self._query, self._path) == other

    def __ne__(self, other) -> DBSQL:
        return DBSQL(self._query, self._path) != other

    def __contains__(self, item) -> DBSQL:
        return typing.cast(DBSQL, DBSQL(self._query, self._path) in item)

    @property
    def pk(self):
        return getattr(self, self._table.DB.pk.name)


class DBSubqueryField:
    def __init__(self, query: DBQuery, subquery: DBQuery, path: str = None):
        self._query: DBQuery = query
        self._subquery: DBQuery = subquery
        self._path: str = path or subquery.name

    def __getattr__(self, item):
        if item.startswith('_'):
            return super().__getattribute__(item)

        if self._path not in self._query.joins:
            self._query.joins[self._path] = DBJoin(self._subquery, DBJoinKind.SOURCE)

        if item in self._subquery.fields:
            return DBSQL(self._query, f'{self._path}.{item}')

        raise QuazyFieldNameError(f'field {item} not found in query {self._subquery.__name__}')

    def __str__(self):
        return self._path

    def __eq__(self, other) -> DBSQL:
        raise QuazyWrongOperation

    def __ne__(self, other) -> DBSQL:
        raise QuazyWrongOperation

    def __contains__(self, item) -> DBSQL:
        raise QuazyWrongOperation


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

    def func3(self, op: str, second: Any, third: Any) -> DBSQL:
        return self.sql(f'{op}({self.sql_text}, {self.query.arg(second)!r}, {self.query.arg(third)!r})')

    def cast(self, type_name: str) -> DBSQL:
        return self.sql(f'{self.sql_text}::{type_name}')
    
    def postfix(self, sql_text: str) -> DBSQL:
        return self.sql(f'{self.sql_text} {sql_text}')

    def __getitem__(self, item):
        if item is int:
            return self.sql(f'{self.sql_text}[{item}]')
        elif isinstance(item, slice):
            return self.substr(item.start, item.stop-item.start+1)
        else:
            return self.sql(f"{self.sql_text}->'{item}'")

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

    def as_string(self) -> DBSQL:
        return self.cast('text')

    def __str__(self):
        return self.sql_text

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

    def upper(self) -> DBSQL:
        return self.func1('upper')

    def lower(self) -> DBSQL:
        return self.func1('lower')

    def __len__(self):
        return self.func2('length', 'UTF8')

    def left(self, n: int) -> DBSQL:
        return self.func2('left', n)

    def right(self, n: int) -> DBSQL:
        return self.func2('right', n)

    def startswith(self, s: str) -> DBSQL:
        return self.left(len(s)) == s

    def endswith(self, s: str) -> DBSQL:
        return self.right(len(s)) == s

    def substr(self, pos: int, length: int = None) -> DBSQL:
        if length is None:
            return self.func2('substr', pos)
        else:
            return self.func3('substr', pos, length)


class DBJoinKind(Enum):
    SOURCE = "SOURCE"  # no join, base table to select
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    INNER = "INNER"
    OUTER = "OUTER"


@dataclass
class DBJoin:
    source: Union[type[DBTable], DBQuery]
    kind: DBJoinKind
    condition: Optional[Union[str, DBSQL]] = None


@dataclass
class DBWithClause:
    query: DBQuery
    not_materialized: bool


class DBScheme(SimpleNamespace):
    pass


if typing.TYPE_CHECKING:
    FDBSQL = DBSQL | Callable[[SimpleNamespace], DBSQL] | str | int


class DBQuery(typing.Generic[T]):
    queries: ClassVar[dict[Hashable, DBQuery]] = {}

    class SaveException(Exception):
        pass

    def __init__(self, db: DBFactory, table_class: Optional[type[T]] = None, name: str = ''):
        self.name = name or f'q{id(self)}'
        self.db: DBFactory = db
        self.table_class: type[T] | None = table_class
        self.fields: OrderedDict[str, DBSQL] = OrderedDict()
        self.fetch_objects: bool = table_class is not None
        self.joins: OrderedDict[str, DBJoin] = OrderedDict()
        self.sort_list: list[DBSQL] = []
        self.filters: list[DBSQL] = []
        self.groups: list[DBSQL] = []
        self.group_filters: list[DBSQL] = []
        self.has_aggregates: bool = False
        self.window = (None, None)
        self.with_queries: list[DBWithClause] = []
        self.args: OrderedDict[str, Any] = OrderedDict()
        self._arg_counter = 0
        self._hash: Optional[Hashable] = None

        self.scheme: Union[SimpleNamespace, DBQueryField] = DBScheme()
        for table in self.db._tables:
            setattr(self.scheme, table.DB.snake_name, DBQueryField(self, table))

        if table_class is not None:
            self.joins[table_class.DB.snake_name] = DBJoin(table_class, DBJoinKind.SOURCE)
            table_space = DBQueryField(self, table_class)
            setattr(table_space, '_db', self.scheme)
            self.scheme = table_space

            if table_class.DB.extendable:
                self.filters.append(getattr(table_space, table_class.DB.cid.name) == self.arg(table_class.DB.discriminator))

    def __copy__(self):
        obj = object.__new__(DBQuery)
        deep_attrs = 'fields joins sort_list filters groups group_filters with_queries args'.split()
        for k, v in self.__dict__.items():
            if k == "name":
                obj.name = f'q{id(obj)}'
            elif k not in deep_attrs:
                setattr(obj, k, v)
            else:
                setattr(obj, k, v.copy())
        return obj

    def copy(self):
        obj = copy.copy(self)
        return obj

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

    def arg(self, value: Any, aggregated: bool = False) -> DBSQL:
        if isinstance(value, DBSQL):
            return value
        if isinstance(value, str):
            return DBSQL(self, f"'{value}'")
        if value is None:
            return DBSQL(self, 'NULL')
        if isinstance(value, DBTable):
            value = value.pk
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

    def sql(self, expr: FDBSQL, scheme: SimpleNamespace = None) -> DBSQL:
        if callable(expr):
            return expr(self.scheme)
        if isinstance(expr, DBSQL):
            return expr
        if isinstance(expr, str):
            if not expr:
                raise QuazyFieldTypeError('Expression is empty string')
            chunks = expr.split('.')
            value = getattr(self.scheme, chunks[0]) if not scheme else getattr(scheme, chunks[0])
            if len(chunks) == 1:
                return value
            return self.sql(expr[expr.index('.')+1:], value)
        if type(expr) is int:
            return DBSQL(self, expr)
        raise QuazyFieldTypeError('Expression type not supported')

    def with_query(self, subquery: DBQuery, not_materialized: bool = False) -> DBSubqueryField:
        self.with_queries.append(DBWithClause(subquery, not_materialized))
        for k, v in subquery.args.items():
            if k.startswith('_arg_'):
                self.args[f'_{subquery.name}{k}'] = v
            else:
                self.args[k] = v
        return DBSubqueryField(self, subquery)

    def select(self, *field_names: str, **fields: FDBSQL) -> DBQuery[T]:
        if self.fetch_objects:
            if 'pk' not in field_names:
                self.fetch_objects = False
            else:
                self.fields[self.table_class.DB.pk.name] = self.scheme.pk
                field_names = set(field_names) - {'pk'}
        for field_name in field_names:
            self.fields[field_name] = getattr(self.scheme, field_name)
        for field_name, field_value in fields.items():
            self.fields[field_name] = self.sql(field_value)
        return self

    def select_all(self) -> DBQuery[T]:
        self.fetch_objects = False
        self.fields['*'] = DBSQL(self, '*')
        return self

    def sort_by(self, *fields: FDBSQL, desc: bool = False) -> DBQuery[T]:
        for field in fields:
            self.sort_list.append(self.sql(field) if not desc else self.sql(field).postfix('DESC'))
        return self

    def filter(self, _expression: FDBSQL = None, **kwargs) -> DBQuery[T]:
        if _expression is not None:
            self.filters.append(self.sql(_expression))
        if kwargs and self.table_class is None:
            raise QuazyError('Query is not associated with table, cat not filter by field names')
        for k, v in kwargs.items():
            self.filters.append(getattr(self.scheme, k) == v)  # noqa
        return self

    def exclude(self, **kwargs) -> DBQuery[T]:
        if self.table_class in None:
            raise QuazyError('Query is not associated with table, cat not filter by field names')
        for k, v in kwargs.items():
            self.filters.append(getattr(self.scheme, k) != v)  # noqa
        return self

    def group_filter(self, expression: FDBSQL) -> DBQuery[T]:
        self.group_filters.append(self.sql(expression))
        return self

    def group_by(self, *fields: FDBSQL) -> DBQuery[T]:
        for field in fields:
            self.groups.append(DBSQL(self, self.sql(field)))
        return self

    def set_window(self, offset: int | None = None, limit: int | None = None) -> DBQuery[T]:
        self.window = (offset, limit)
        return self

    def sum(self, expr: DBSQL | str | typing.Callable[[T], DBSQL]) -> DBSQL:
        self.has_aggregates = True
        expr = self.sql(expr)
        return expr.aggregate('sum')

    def count(self, expr: FDBSQL = None) -> DBSQL:
        if expr is None:
            expr = DBSQL(self, '*')
        else:
            expr = self.sql(expr)
        self.has_aggregates = True
        return expr.aggregate('count')

    def avg(self, expr: FDBSQL) -> DBSQL:
        self.has_aggregates = True
        expr = self.sql(expr)
        return expr.aggregate('avg')

    def min(self, expr: DBSQL) -> DBSQL:
        self.has_aggregates = True
        expr = self.sql(expr)
        return expr.aggregate('min')

    def max(self, expr: DBSQL) -> DBSQL:
        self.has_aggregates = True
        expr = self.sql(expr)
        return expr.aggregate('max')

    def _check_fields(self):
        if not self.fields:
            if not self.fetch_objects:
                raise QuazyError('No fields selected')
            else:
                for field_name, field in self.table_class.DB.fields.items():
                    if not field.body:
                        self.fields[field_name] = getattr(self.scheme, field_name)

    @contextmanager
    def execute(self, as_dict: bool = False):
        self._check_fields()
        with self.db.select(self, as_dict) as curr:
            yield curr

    def describe(self) -> list[DBField]:
        self._check_fields()
        return self.db.describe(self)

    def __iter__(self):
        self._check_fields()
        with self.db.select(self) as rows:
            yield from rows

    def fetchone(self, as_dict: bool = False) -> T | Any:
        with self.execute(as_dict) as curr:
            return curr.fetchone()

    def get(self, pk_id: Any) -> T | None:
        if not self.fetch_objects:
            raise QuazyWrongOperation("`get` possible for objects query")
        self.filters.clear()
        self.filters.append(self.scheme.pk == pk_id)  # type: ignore
        return self.fetchone()

    def __getitem__(self, item: Any) -> T | None:
        return self.get(item)

    def fetchall(self, as_dict: bool = False) -> list[T | Any]:
        with self.execute(as_dict) as curr:
            return curr.fetchall()

    def fetchvalue(self) -> Any:
        with self.execute() as curr:
            if (one:=curr.fetchone()) is not None:
                return one[0]
            return None

    def fetchlist(self) -> list[Any]:
        with self.execute() as curr:
            return [row[0] for row in curr.fetchall()]

    def exists(self) -> bool:
        return self.fetchone() is not None

    def fetch_aggregate(self, function: str, expr: FDBSQL = None) -> typing.Any:
        obj = self.copy()
        obj.fields.clear()
        obj.fetch_objects = False
        obj.select(result=obj.sql(expr).aggregate(function))
        return obj.fetchone().result

    def fetch_count(self, expr: FDBSQL = None):
        obj = self.copy()
        obj.fields.clear()
        obj.fetch_objects = False
        obj.select(result=obj.count(expr))
        return obj.fetchone().result

    def fetch_max(self, expr: FDBSQL) -> typing.Any:
        return self.fetch_aggregate('max', expr)

    def fetch_min(self, expr: FDBSQL) -> typing.Any:
        return self.fetch_aggregate('min', expr)

    def fetch_avg(self, expr: FDBSQL) -> typing.Any:
        return self.fetch_aggregate('avg', expr)

