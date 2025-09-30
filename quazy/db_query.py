from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import dataclass
import typing
from inspect import currentframe
from types import SimpleNamespace
from collections import OrderedDict
from enum import Enum
import copy

from .db_factory import DBFactory, T
from .db_table import DBTable
from .db_field import DBField
from .exceptions import *

if typing.TYPE_CHECKING:
    from typing import *


__all__ = ['DBQuery', 'DBScheme', 'DBQueryField']

def is_expression_id(expr: str) -> str:
    # check expression is field "name"
    return is_expression_id.r.fullmatch(expr) is not None

is_expression_id.r = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]+")

def is_expression_canonical(expr: str) -> bool:
    # check expression is "some.field.name"
    return is_expression_canonical.r.fullmatch(expr) is not None

is_expression_canonical.r = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]+([.][a-zA-Z_][a-zA-Z0-9_]+)+$")

class DBQueryField(typing.Generic[T]):
    def __init__(self, query: DBQuery, table: type[T], path: str = None, field: DBField = None):
        self._query: DBQuery[T] = query
        self._table: type[T] = table
        self._path: str = path or table.DB.table
        self._field: DBField = field

    def __getattr__(self, item):
        if item.startswith('_'):
            return super().__getattribute__(item)

        if not self._field:
            if self._path not in self._query.joins:
                self._query.joins[self._path] = DBJoin(DBJoinKind.SOURCE, self._table)
        else:
            join_path = f'{self._table.DB.table}__{self._field.name}s'
            if join_path not in self._query.joins:
                join_kind = DBJoinKind.INNER if self._field.required else DBJoinKind.LEFT
                self._query.joins[join_path] = DBJoin(join_kind, self._field.type,
                                                      f'{self._path} = "{join_path}".{self._field.type.DB.pk.name}')
            return getattr(DBQueryField(self._query, self._field.type, join_path), item)

        DB = self._table.DB
        if item in DB.fields:
            field: DBField = DB.fields[item]
            if not field.property:
                field_path = f'"{self._path}".{item}'
            else:
                field_path = f'"{self._path}".{DB.body.name}'
                field_path = self._query.db._trans.json_deserialize(field, f"{field_path}->>'{item}'")

            if field.ref:
                return DBQueryField(self._query, self._table, field_path, field)
            return DBSQL(self._query, f'{field_path}')

        elif table := DB.subtables.get(item):
            join_path = f'{self._path}__{item}'
            if join_path not in self._query.joins:
                self._query.joins[join_path] = DBJoin(DBJoinKind.INNER, table,
                                                  f'"{self._path}".{DB.pk.name} = "{join_path}".{DB.table}')
            return DBQueryField(self._query, table, join_path)

        elif  many_field := DB.many_fields.get(item):
            join_path = f'{self._path}__{item}'
            if join_path not in self._query.joins:
                self._query.joins[join_path] = DBJoin(DBJoinKind.INNER, many_field.foreign_table,
                                                  f'"{self._path}".{DB.pk.name} = "{join_path}".{many_field.foreign_field}')
            return DBQueryField(self._query, many_field.foreign_table, join_path)

        elif many_to_many_field := DB.many_to_many_fields.get(item):
            join_path_middle = f'{many_to_many_field.middle_table.DB.table}'
            if join_path_middle not in self._query.joins:
                self._query.joins[f'{join_path_middle}'] = DBJoin(DBJoinKind.INNER, many_to_many_field.middle_table,
                                                  f'"{self._path}".{DB.pk.name} = "{join_path_middle}".{self._table.DB.table}')
            join_path = f'{self._path}__{item}'
            if join_path not in self._query.joins:
                self._query.joins[join_path] = DBJoin(DBJoinKind.INNER, many_to_many_field.foreign_table,
                                                  f'"{join_path_middle}".{many_to_many_field.foreign_table.DB.table} = "{join_path}".{many_to_many_field.foreign_table.DB.pk.name}')
            return DBQueryField(self._query, many_to_many_field.foreign_table, join_path)

        raise QuazyFieldNameError(f'field `{item}` is not found in `{DB.table}`')
    
    def __getitem__(self, item):
        return getattr(self, item)

    def __str__(self):
        return self._path

    def __eq__(self, other) -> DBSQL:
        return DBSQL(self._query, self._path) == other

    def __ne__(self, other) -> DBSQL:
        return DBSQL(self._query, self._path) != other

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
            self._query.joins[self._path] = DBJoin(DBJoinKind.SOURCE, self._subquery)

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


class DBConditionField:
    def __init__(self, query: DBQuery):
        self._query: DBQuery = query
        self._conditions: list[tuple[FDBSQL, FDBSQL]] = []
        self._default: FDBSQL = None

    def condition(self, value: FDBSQL, condition: FDBSQL) -> typing.Self:
        self._conditions.append((self._query.resolve(condition), self._query.resolve(value)))
        return self

    def default(self, expr: FDBSQL) -> typing.Self:
        self._default = expr
        return self

    def build(self) -> DBSQL:
        if not self._default:
            raise QuazyWrongOperation("No default condition specified")
        if not self._conditions:
            return self._query.resolve(self._default)
        results = [
            "WHEN {} THEN {}".format(*condition) for condition in self._conditions
        ]
        return DBSQL(self._query, "CASE\n" + "\n".join(results) + "\nEND")

    def __call__(self, *args, **kwargs):
        return self.build()


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

    #def arg(self, value: Any) -> DBSQL:
    #    return self.query.arg(value, self.aggregated)
    
    def func1(self, op: str) -> DBSQL:
        return self.sql(f'{op}({self.sql_text})')

    def aggregate(self, op: str) -> DBSQL:
        self.aggregated = True
        return self.func1(op)

    def op(self, op: str, other: Any) -> DBSQL:
        return self.sql(f'{self.sql_text}{op}{self.query.arg(other)!r}')

    def op_rev(self, op: str, other: Any) -> DBSQL:
        return self.sql(f'{self.query.arg(other)!r}{op}{self.sql_text}')

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
        return self.func1("NOT")

    def invert(self) -> DBSQL:
        return self.func1('~')

    def __lshift__(self, other) -> DBSQL:
        return self.op(" IN ", other)

    def lshift(self, other) -> DBSQL:
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

    @property
    def as_string(self) -> DBSQL:
        return self.cast('text')

    def __str__(self):
        return self.sql_text

    #def __int__(self) -> DBSQL:
    @property
    def as_integer(self) -> DBSQL:
        return self.cast('integer')

    #def __float__(self) -> DBSQL:
    @property
    def as_float(self) -> DBSQL:
        return self.cast('double precision')

    #def __bool__(self) -> DBSQL:
    #    return self.cast('bool')

    def __round__(self, n=None) -> DBSQL:
        return self.func2('round', n)

    def __trunc__(self) -> DBSQL:
        return self.func1('trunc')

    #def __contains__(self, item) -> DBSQL:
    #    return self.contains(item)

    def contains(self, item) -> DBSQL:
        return self.sql('{} LIKE {!r}'.format(self.sql_text, self.query.arg(f'%{item}%')))

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

    def min(self):
        return self.aggregate('min')

    def max(self):
        return self.aggregate('max')

    def avg(self):
        return self.aggregate('avg')

    def count(self):
        return self.aggregate('count')


class DBJoinKind(Enum):
    SOURCE = "SOURCE"  # no join, base table to select
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    INNER = "INNER"
    OUTER = "OUTER"


@dataclass
class DBJoin:
    kind: DBJoinKind
    with_table: Union[type[DBTable], DBQuery]
    condition: Optional[Union[str, DBSQL]] = None


@dataclass
class DBWithClause:
    query: DBQuery
    not_materialized: bool

class DBChainedFilter(typing.NamedTuple):
    id_name: str
    next_name: str
    sql_value: Any

class DBScheme(SimpleNamespace):
    pass


if typing.TYPE_CHECKING:
    type FDBSQL = DBSQL | Callable[[SimpleNamespace], DBSQL] | str | int | DBConditionField


class DBQuery(typing.Generic[T]):
    """Query base class

    Create it with DBFactory.query() or DBTable.query().
    """
    queries: ClassVar[dict[Hashable, DBQuery]] = {}

    class SaveException(Exception):
        """inner signal for reusable queries

        :meta private:
        """

    def __init__(self, db: DBFactory, table_class: Optional[type[T]] = None, name: str = ''):
        """Constructor

        Arguments:
            db: DB factory
            table_class: `DBTable` class to bind a query to specific table
            name: internal name of this query to use within subqueries
        """
        self.name = name or f'q{id(self)}'
        self.db: DBFactory = db
        self.table_class: type[T] | None = table_class
        self.fields: OrderedDict[str, DBSQL] = OrderedDict()
        self.fetch_objects: bool = table_class is not None
        self.joins: OrderedDict[str, DBJoin] = OrderedDict()
        self.sort_list: list[DBSQL] = []
        self.filters: list[DBSQL] = []
        self.chained_opts: DBChainedFilter | None = None
        self.groups: list[DBSQL] = []
        self.group_filters: list[DBSQL] = []
        self.has_aggregates: bool = False
        self.window: tuple[int | None, int | None] = (None, None)
        self.is_distinct: bool = False
        self.with_queries: list[DBWithClause] = []
        self.args: dict[str, Any] = {}
        self._arg_counter: int = 0
        self._hash: Optional[Hashable] = None
        self._collect_scheme()

    def _collect_scheme(self, for_copy: bool = False):
        self.scheme: Union[SimpleNamespace, DBQueryField[T]] = DBScheme()
        for table in self.db._tables:
            setattr(self.scheme, table.DB.snake_name, DBQueryField(self, table))

        if self.table_class is not None:
            if not for_copy:
                self.joins[self.table_class.DB.table] = DBJoin(DBJoinKind.SOURCE, self.table_class)
            table_space = DBQueryField(self, self.table_class)
            setattr(table_space, '_db', self.scheme)
            self.scheme = table_space

            if not for_copy and self.table_class.DB.extendable:
                self.filters.append(getattr(table_space, self.table_class.DB.cid.name) == self.arg(self.table_class.DB.discriminator))

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
        obj._collect_scheme(True)
        return obj

    def copy(self):
        """Make a copy of a query"""
        obj = copy.copy(self)
        return obj

    def __enter__(self) -> DBQuery:
        return self

    def reuse(self):
        """Put context generated query into the hash"""
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
    def get_scheme(self) -> SimpleNamespace | DBQueryField[T]:
        """Scheme object for query context

        Scheme contains
         * snake names of all tables assigned to a database, if a query is not bound to a table
         * all fields otherwise

        Each attribute of a scheme works as an expression generator.
        """
        yield self.scheme

    def arg(self, value: Any, aggregated: bool = False) -> DBSQL:
        """Convert any value to a part of the expression

        Arguments:
            value: any value to convert
            aggregated: value is used in aggregated expressions

        :meta private:
        """
        if isinstance(value, DBSQL):
            return value
        #if isinstance(value, str):
        #    return DBSQL(self, f"'{value}'")
        if value is None:
            return DBSQL(self, 'NULL')
        if isinstance(value, DBTable):
            value = value.pk
        if value in self.args.values():
            key = next(k for k, v in self.args.items() if v == value)
            #key = list(self.args.keys())[list(self.args.values()).index(value)]
            return DBSQL(self, f'%({key})s', aggregated)
        self._arg_counter += 1
        key = f'_arg_{self._arg_counter}'
        self.args[key] = value
        return DBSQL(self, f'%({key})s', aggregated)

    def var(self, key: str, value: Optional[Any] = None) -> DBSQL:
        """Define variable to pass to query.

        Put variable to a query to avoid big query reconstruction.

        Arguments:
            key: variable name
            value: variable value

        Example:
            .. code-block:: python

                q = db.query(Figures).select("name")
                q.filter(lambda x: x.angles == q.var('angles'))
                for angle in range(3, 7):
                    q['angle'] = angle
                    print(q.fetch_one())
        """
        self.args[key] = value
        return DBSQL(self, f'%({key})')

    def __setitem__(self, key, value):
        """Set variable to value

        :meta public:
        """
        if key not in self.args:
            raise QuazyWrongOperation(f"No such variable `{key}`")
        self.args[key] = value

    def resolve(self, expr: FDBSQL, scheme: SimpleNamespace = None) -> DBSQL:
        """Makes lambdas, strings and integers as a part of the expression

        This method is intended to process filters, sorting and select expressions.

        :meta private:
        """
        if not scheme:
            scheme = self.scheme
        if callable(expr):
            return expr(scheme)
        if isinstance(expr, DBSQL):
            return expr
        if isinstance(expr, str):
            if not expr:
                raise QuazyFieldTypeError('Expression is empty string')
            if not is_expression_canonical(expr):
                if self.table_class is not None and expr in self.table_class.DB.fields:
                    return getattr(scheme, expr)
                return DBSQL(self, expr)
            chunks = expr.split('.')
            sub_scheme = getattr(scheme, chunks[0])
            if len(chunks) == 1:
                return sub_scheme
            return self.resolve(expr[expr.index('.') + 1:], sub_scheme)
        if type(expr) is int:
            return DBSQL(self, expr)
        if isinstance(expr, DBConditionField):
            return expr.build()
        if isinstance(expr, DBTable):
            if not self.table_class:
                raise QuazyFieldTypeError('Table is not bound to a query')
            if self.table_class != expr:
                raise QuazyFieldTypeError(f'Can not filter table `{self.table_class.__qualname__}` by the instance of `{expr.__class__.__qualname__}`')
            return scheme.pk == expr
        raise QuazyFieldTypeError('Expression type not supported')

    def with_query(self, subquery: DBQuery, not_materialized: bool = False) -> DBSubqueryField:
        """Use another query result field for this query.

        Example:
            .. code-block:: python

                q = db.query(Sales).select("date", "sum")
                q2 = db.query()
                sub = q2.with_query(q)
                q2.select(total_sum=q2.sum(sub.sum))

        Arguments:
            subquery: subquery to use
            not_materialized: ask the database engine to not request a whole query result set

        Returns:
            `DBSubqueryField` with result field names directly accessible for expressions
        """
        self.with_queries.append(DBWithClause(subquery, not_materialized))
        for k, v in subquery.args.items():
            if k.startswith('_arg_'):
                self.args[f'_{subquery.name}{k}'] = v
            else:
                self.args[k] = v
        return DBSubqueryField(self, subquery)

    def select(self, *field_names: str, **fields: FDBSQL) -> DBQuery[T]:
        """Specify a list of selected fields

        Don't call this method if you want to fetch a list of `DBTable` instances (with all fields).
        Otherwise, include 'pk' in `field_names` or you will get a list of named tuples.

        Arguments:
            *field_names: names of fields to select
            **fields: fields to select, where values can be lambdas

        Returns:
            `DBQuery` for chain calls
        """
        if self.fetch_objects:
            if 'pk' not in field_names:
                self.fetch_objects = False
            else:
                self.fields[self.table_class.DB.pk.name] = self.scheme.pk
                field_names = set(field_names) - {'pk'}
        for field_name in field_names:
            self.fields[field_name] = self.resolve(field_name)
        for field_name, field_value in fields.items():
            self.fields[field_name] = self.resolve(field_value)
        return self

    def select_all(self) -> DBQuery[T]:
        """Select all possible fields for this query.

        This is similar to a `SELECT * FROM ...` query.

        Note:
            This method prevents fetching `DBTable` instances to avoid collision with specific fields.
            Use `select_objects` instead.

        Returns:
            `DBQuery` for chain calls
        """
        self.fetch_objects = False
        self.fields['*'] = DBSQL(self, '*')
        return self

    def select_objects(self) -> DBQuery[T]:
        """Select all fields specified for this query.

        Returns:
            `DBQuery` for chain calls
        """
        self._check_fields()
        return self

    def distinct(self) -> DBQuery[T]:
        """Select only different rows for this query.

        Add a `DISTINCT` clause to a `SELECT ...` statement.
        """
        self.is_distinct = True
        return self

    def sort_by(self, *fields: FDBSQL, desc: bool = False) -> DBQuery[T]:
        """Add sorting to a query

        Arguments:
            *fields: fields to sort, can be field name, field number or lambda expression
            desc: sort ascending if False

        Returns:
            `DBQuery` for chain calls
        """
        for field in fields:
            self.sort_list.append(self.resolve(field) if not desc else self.resolve(field).postfix('DESC'))
        return self

    def filter(self, _expression: FDBSQL | DBTable = None, **kwargs) -> DBQuery[T]:
        """Add filter to a query

        Filter can be applied by a common lambda expression or by specific field/value pairs.

        Hint:
            Use identical method name `where` for your preference.

        Example:
            .. code-block:: python

                just_teens = Kids.select().filter(age=18)
                older_then = Kids.select().filter(lambda x: x.age > 18)

        Arguments:
            _expression: lambda expression to filter
            **kwargs: field/value pairs to filter

        Returns:
            `DBQuery` for chain calls
        """
        if _expression is not None:
            sql = self.resolve(_expression)
            if sql.aggregated:
                self.group_filters.append(sql)
            else:
                self.filters.append(sql)
        if kwargs and self.table_class is None:
            raise QuazyError('Query is not associated with table, cat not filter by field names')
        for k, v in kwargs.items():
            self.filters.append(getattr(self.scheme, k) == v)  # noqa
        return self

    where = filter

    def exclude(self, _expression: FDBSQL = None, **kwargs) -> DBQuery[T]:
        """Filter elements to exclude from a query

        Works like a negative filter (excluding elements from a selection)

        Example:
            .. code-block:: python

                no_teens = Kids.select().exclude(age=18)
                youngsters = Kids.select().exclude(lambda x: x.age > 18)

        Arguments:
            _expression: lambda expression to filter
            **kwargs: field/value pairs to filter

        Returns:
            `DBQuery` for chain calls
        """
        if _expression is not None:
            sql = ~self.resolve(_expression)
            if sql.aggregated:
                self.group_filters.append(sql)
            else:
                self.filters.append(sql)
        if self.table_class in None:
            raise QuazyError('Query is not associated with table, cat not filter by field names')
        for k, v in kwargs.items():
            self.filters.append(getattr(self.scheme, k) != v)  # noqa
        return self

    def group_filter(self, expression: FDBSQL) -> DBQuery[T]:
        """Filter applied to group fields. See below

        Hint:
            This method is not necessary to call, because expression resolver detects aggregated functions calls
            automatically.

        Arguments:
            expression: lambda expression to filter

        Returns:
            `DBQuery` for chain calls
        """
        self.group_filters.append(self.resolve(expression))
        return self

    def group_by(self, *fields: FDBSQL) -> DBQuery[T]:
        """Specify group fields for aggregated results

        This is a query analogue to `GROUP BY ...` statement.

        Arguments:
            *fields: list of field names or expressions to group

        Returns:
            `DBQuery` for chain calls
        """
        for field in fields:
            self.groups.append(self.resolve(field))
        return self

    def set_window(self, offset: int | None = None, limit: int | None = None) -> DBQuery[T]:
        """Set the query result window using SQL offset/limit features

        This is analogue to `SELECT a, b, c FROM table OFFSET ... LIMIT ...` statement.
        """
        self.window = (offset, limit)
        return self

    def sum(self, expr: FDBSQL) -> DBSQL:
        """Use aggregated function `sum` as a part of the expression

        Example:
            .. code-block:: python

                q = db.query(Posts)
                q = q.group_by("topic").select("topic", total_views=q.sum("views_counter"))
        """
        self.has_aggregates = True
        expr = self.resolve(expr)
        return expr.aggregate('sum')

    def count(self, expr: FDBSQL = None) -> DBSQL:
        """Use aggregated function `count` as a part of the expression

        If no argument is specified, count all result rows.

        Example:
            .. code-block:: python

                q = db.query(Posts)
                q = q.group_by("topic").select("topic", total_views=q.count())
        """
        if expr is None:
            expr = DBSQL(self, '*')
        else:
            expr = self.resolve(expr)
        self.has_aggregates = True
        return expr.aggregate('count')

    def avg(self, expr: FDBSQL) -> DBSQL:
        """Use aggregated function `avg` (average) as a part of the expression"""
        self.has_aggregates = True
        expr = self.resolve(expr)
        return expr.aggregate('avg')

    def min(self, expr: DBSQL) -> DBSQL:
        """Use aggregated function `min` as a part of the expression"""
        self.has_aggregates = True
        expr = self.resolve(expr)
        return expr.aggregate('min')

    def max(self, expr: DBSQL) -> DBSQL:
        """Use aggregated function `max` as a part of the expression"""
        self.has_aggregates = True
        expr = self.resolve(expr)
        return expr.aggregate('max')

    def case(self) -> DBConditionField:
        """Make `DBConditionField` object for conditional values

        This is an analogue to SQL `CASE ...` statement.

        Example:
             .. code-block:: python

                q = db.query(User)
                c = q.case().
                    condition("baby", lambda x: x.age <= 1).
                    condition("toddler", lambda x: x.age <= 3).
                    condition("kid", lambda x: x.age < 18).
                    default("adult")
                q.select("name", age_category=c)
        """
        return DBConditionField(self)

    def _check_fields(self):
        """check and refill fields if no fields selected"""
        if not self.fields:
            if not self.fetch_objects:
                raise QuazyError('No fields selected')
            else:
                for field_name, field in self.table_class.DB.fields.items():
                    if not field.body:
                        self.fields[field_name] = getattr(self.scheme, field_name)

    @contextmanager
    def execute(self, as_dict: bool = False):
        """Execute query and yields database cursor to fetch one or more result rows.

        Arguments:
            as_dict: whether to return dict instead of DBTable/SimpleNamespace

        Yields:
            database cursor
        """
        self._check_fields()
        with self.db.select(self, as_dict) as curr:
            yield curr

    def describe(self) -> list[DBField]:
        """Request all result fields information.

        See `DBFactory.describe()`
        """
        self._check_fields()
        return self.db.describe(self)

    def __iter__(self) -> Generator[T]:
        """Execute a query and iterate all over result rows

        :meta public:
        """
        with self.execute() as curr:
            yield from curr

    def fetch_one(self, as_dict: bool = False) -> T | Any:
        """Execute query and fetch first result row"""
        with self.execute(as_dict) as curr:
            return curr.fetchone()

    def get(self, pk_id: Any) -> T | None:
        """Request and get one row by the primary key identifier"""
        if not self.fetch_objects:
            raise QuazyWrongOperation("`get` possible for objects query")
        self.filters.clear()
        self.filters.append(self.scheme.pk == pk_id)  # type: ignore
        return self.fetch_one()

    @classmethod
    def any(cls, expr_list: typing.Iterator[DBSQL]) -> DBSQL:
        """Produce expression with several alternatives.

        This is analogue to `ex1 OR ex2 OR ex3 ...`

        Arguments:
            expr_list: list, tuple or other iterator of expressions

        Example:
            .. code-block:: python

                colors = ('green', 'yellow', 'red')
                q = db.query(Apple)
                q.filter(q.any(lambda x: x.color == color for color in colors))
        """
        result = next(expr_list, None)
        while (expr:=next(expr_list, None)) is not None:
            result = result | expr
        return result

    def __getitem__(self, item: Any) -> T | None:
        """Short form to get item by primary key identifier"""
        return self.get(item)

    def fetch_all(self, as_dict: bool = False) -> list[T | Any]:
        """Execute a query and fetch all result rows as a list"""
        with self.execute(as_dict) as curr:
            return curr.fetchall()

    def fetch_value(self) -> Any:
        """Execute a query and fetch first column value of first result row"""
        with self.execute() as curr:
            if (one:=curr.fetchone()) is not None:
                return one[0]
            return None

    def fetch_list(self, index: int | str = 0) -> list[Any]:
        """Execute a query and fetch the first column of all result rows as a list of values

        Arguments:
            index: column index or name
        """
        with self.execute() as curr:
            rows = curr.fetchall()
            if type(index) is int:
                return [row[index] for row in rows]
            else:
                return [getattr(row, index) for row in rows]

    def exists(self) -> bool:
        """Execute a query and check whether the first result row exists"""
        return self.fetchone() is not None

    def fetch_aggregate(self, function: str, expr: FDBSQL = None) -> typing.Any:
        """Execute subquery to fetch aggregate function result value

        This group of functions is intended to estimate query metrics and numbers before real execution.

        Example:
            .. code-block:: python

                q = db.query(Posts).filter(lambda x: x.created_at >= datetime.now() - timedelta(days=10))
                print(q.fetch_count())

        Arguments:
            function: SQL-friendly aggregate function name
            expr: any expression, like lambdas or DBSQL

        Returns:
            integer or float requested value
        """
        obj = self.copy()
        obj.fields.clear()
        obj.fetch_objects = False
        obj.select(result=obj.resolve(expr).aggregate(function))
        return obj.fetch_one().result

    def fetch_count(self, expr: FDBSQL = None):
        """Execute subquery to fetch aggregate function `count` result value"""
        obj = self.copy()
        obj.fields.clear()
        obj.fetch_objects = False
        obj.select(result=obj.count(expr))
        return obj.fetch_one().result

    def fetch_sum(self, expr: FDBSQL) -> typing.Any:
        """Execute subquery to fetch aggregate function `sum` result value"""
        return self.fetch_aggregate('sum', expr)

    def fetch_max(self, expr: FDBSQL) -> typing.Any:
        """Execute subquery to fetch aggregate function `max` result value"""
        return self.fetch_aggregate('max', expr)

    def fetch_min(self, expr: FDBSQL) -> typing.Any:
        """Execute subquery to fetch aggregate function `min` result value"""
        return self.fetch_aggregate('min', expr)

    def fetch_avg(self, expr: FDBSQL) -> typing.Any:
        """Execute subquery to fetch aggregate function `avg` result value"""
        return self.fetch_aggregate('avg', expr)

    def update(self, **values) -> DBQuery[T]:
        """Updates the current query object with the specified values"""
        return self.db.update_many(self, **values)

    def chained(self, id_name: str, next_name: str, start_value: Any) -> DBQuery[T]:
        """Select chained rows via recursive request

        Arguments:
            id_name: name of the field with an original identifier
            next_name: name of the field with an identifier of the next row
            start_value: starting identifier value for the first row in the chain

        Example:
            ..  code-block:: python

                class Chained(DBTable):
                    index: int
                    next: int
                    name: str

                q = Chained.chained("index", "next", 1)
                print(q.fetch_all())
        """
        if self.table_class is None:
            raise QuazyWrongOperation("Query is not bound to a table")
        if id_name not in self.table_class.DB.fields:
            raise QuazyFieldNameError(f'Field `{id_name}` is not defined in table `{self.table_class.__qualname__}`')
        if next_name not in self.table_class.DB.fields:
            raise QuazyFieldNameError(f'Field `{next_name}` is not defined in table `{self.table_class.__qualname__}`')
        if not self.fetch_objects:
            self.select(id_name, next_name) # at least `next_name` field must be selected
        self.chained_opts = DBChainedFilter(id_name, next_name, self.arg(start_value))
        return self