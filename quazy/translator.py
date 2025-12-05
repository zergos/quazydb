from __future__ import annotations

from abc import ABC, abstractmethod
import typing

if typing.TYPE_CHECKING:
    from typing import *

    from .db_table import DBTable
    from .db_field import DBField
    from .db_query import DBQuery, DBSQL, DBQueryField, DBWithClause, DBSubqueryField

class Translator(ABC):
    TYPES_MAP = {}

    TYPES_BY_OID = {}

    @classmethod
    @abstractmethod
    def create_index(cls, table: type[DBTable], field: DBField) -> str: ...

    @classmethod
    @abstractmethod
    def drop_index(cls, table: type[DBTable], field: DBField) -> str: ...

    @classmethod
    @abstractmethod
    def set_default_value(cls, table: type[DBTable], field: DBField, sql_value: str) -> str: ...

    @classmethod
    @abstractmethod
    def create_schema(cls, name: str): ...

    @classmethod
    @abstractmethod
    def create_table(cls, table: type[DBTable]) -> str: ...

    @classmethod
    @abstractmethod
    def add_field(cls, table: type[DBTable], field: DBField): ...

    @classmethod
    @abstractmethod
    def drop_field(cls, table: type[DBTable], field: DBField): ...

    @classmethod
    @abstractmethod
    def rename_field(cls, table: type[DBTable], old_name: str, new_name: str): ...

    @classmethod
    @abstractmethod
    def alter_field_type(cls, table: type[DBTable], field: DBField): ...

    @classmethod
    @abstractmethod
    def drop_table(cls, table: type[DBTable]) -> str: ...

    @classmethod
    @abstractmethod
    def rename_table(cls, schema: str, old_table_name: str, new_table_name: str) -> str: ...

    @classmethod
    @abstractmethod
    def add_reference(cls, table: type[DBTable], field: DBField) -> str: ...

    @classmethod
    @abstractmethod
    def drop_reference(cls, table: type[DBTable], field: DBField) -> str: ...

    @classmethod
    @abstractmethod
    def set_not_null(cls, table: type[DBTable], field: DBField) -> str: ...

    @classmethod
    @abstractmethod
    def drop_not_null(cls, table: type[DBTable], field: DBField) -> str: ...

    @classmethod
    @abstractmethod
    def insert(cls, item: DBTable, fields: list[tuple[DBField, Any]]) -> tuple[str, dict[str, Any]]: ...

    @classmethod
    @abstractmethod
    def clear(cls, table: type[DBTable]) -> str: ...

    @classmethod
    @abstractmethod
    def delete_related(cls, table: type[DBTable], column: str) -> str: ...

    @classmethod
    @abstractmethod
    def update(cls, table: type[DBTable], fields: list[tuple[DBField, Any]], query: DBQuery | str = None) -> tuple[str, dict[str, Any]]: ...

    @classmethod
    @abstractmethod
    def sql_value(cls, value: Union[DBSQL, DBQueryField, str]) -> str: ...

    @classmethod
    @abstractmethod
    def with_select(cls, with_queries: list[DBWithClause]): ...

    @classmethod
    @abstractmethod
    def select(cls, query: DBQuery, chained_mode: int = 0) -> str: ...

    @classmethod
    @abstractmethod
    def delete(cls, table: type[DBTable]) -> str: ...

    @classmethod
    @abstractmethod
    def delete_selected(cls, query: DBQuery, subquery: DBSubqueryField) -> str: ...

    @classmethod
    @abstractmethod
    def select_all_tables(cls) -> str: ...

    @classmethod
    @abstractmethod
    def is_table_exists(cls, table: DBTable) -> str: ...

    @classmethod
    @abstractmethod
    def select_many_indices(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str: ...

    @classmethod
    @abstractmethod
    def delete_many_indices(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str: ...

    @classmethod
    @abstractmethod
    def insert_many_index(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str: ...
