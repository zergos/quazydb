import inspect
import json
import os
import typing
try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum
from enum import auto
from typing import NamedTuple, Any, Type

from .db import DBFactory, DBTable, DBField
from .query import DBQuery
from .db_types import datetime
from .exceptions import *

__all__ = ["check_migrations", "activate_migrations", "get_migrations", "get_changes", "apply_changes"]

_SCHEMA_ = "migrations"


class MigrationVersion(DBTable):
    schema: str
    index: int


class Migration(DBTable):
    created_at: datetime = DBField(default_sql="now()")
    schema: str
    index: int
    tables: str
    commands: str
    comments: str
    detached: bool = False


def check_migrations(db: DBFactory) -> bool:
    db.use_module(__name__)
    return db.check(_SCHEMA_)


def activate_migrations(db: DBFactory):
    db.use_module(__name__)
    db.create(_SCHEMA_)


def get_migrations(db: DBFactory, schema: str) -> list[tuple[bool, int, datetime, str]]:
    current = db.query(MigrationVersion).filter(lambda x: x.schema == schema).select("index").fetchone()
    current_index = current[0] if current else -1

    res = []
    for row in db.query(Migration).filter(lambda x: x.schema == schema).select("index", "created_at", "comments"):
        res.append((row.index == current_index, row.index, row.created, row.comments))

    return res


class MigrationType(StrEnum):
    INITIAL = auto()
    ADD_TABLE = auto()
    DELETE_TABLE = auto()
    RENAME_TABLE = auto()
    ADD_FIELD = auto()
    DELETE_FIELD = auto()
    RENAME_FIELD = auto()
    ALTER_FIELD_TYPE = auto()
    ALTER_FIELD_FLAG = auto()


class MigrationCommand(NamedTuple):
    command: MigrationType
    subject: tuple[Any, ...]

    def __str__(self):
        match self.command:
            case MigrationType.INITIAL:
                return f"Initial migration"
            case MigrationType.ADD_TABLE:
                return f"Add table {self.subject[0].__qualname__}"
            case MigrationType.DELETE_TABLE:
                return f"Delete table {self.subject[0].__qualname__}"
            case MigrationType.RENAME_TABLE:
                return f"Rename table {self.subject[1]} to {self.subject[2]}"
            case MigrationType.ADD_FIELD:
                return f"Add field {self.subject[1].name} to table {self.subject[0].__qualname__}"
            case MigrationType.DELETE_FIELD:
                return f"Delete field {self.subject[1].name} from table {self.subject[0].__qualname__}"
            case MigrationType.RENAME_FIELD:
                return f"Rename field {self.subject[1]} to {self.subject[2]} at table {self.subject[0].__qualname__}"
            case MigrationType.ALTER_FIELD_TYPE:
                return f"Alter field type {self.subject[1].name} from {self.subject[2]} to {self.subject[3]} at table {self.subject[0].__qualname__}"
            case MigrationType.ALTER_FIELD_FLAG:
                return f"Alter field {self.subject[1].name} flag {self.subject[2]} to value {self.subject[3]} at table {self.subject[0].__qualname__}"
            case _:
                return f"Custom command: {self.command} {self.subject}"

    def save(self) -> dict[str, typing.Any]:
        args = []
        if self.subject:
            for arg in self.subject:
                if inspect.isclass(arg) and issubclass(arg, DBTable):
                    args.append(('DBTable', arg.__qualname__))
                elif isinstance(arg, DBField):
                    args.append(('DBField', arg.name))
                elif isinstance(arg, str):
                    args.append(('str', arg))
                elif type(arg) is bool:
                    args.append(('bool', str(arg)))
                elif arg is None:
                    pass
                else:
                    raise QuazyError('Wrong arg type in command argument')
        return {'command': self.command, 'subject': args}

    @classmethod
    def load(cls, data: dict[str, typing.Any], tables: dict[str, Type[DBTable]]):
        args = []
        for arg in data['subject']:
            if arg[0] == 'DBTable':
                args.append(tables[arg[1]])
            elif arg[0] == 'DBField':
                args.append(args[0]._fields_[arg[0]])
            elif arg[0] == 'str':
                args.append(arg)
            elif arg[0] == 'bool':
                args.append(arg[1] == 'True')
            else:
                raise QuazyError('Wrong arg type in command loading')
        return cls(command=data['command'], subject=tuple(args))


def get_changes(db: DBFactory, schema: str, rename_list: list[tuple[str, str]] | None = None) -> tuple[list[MigrationCommand], list[Type[DBTable]]]:
    db.use_module(__name__)

    commands: list[MigrationCommand] = []

    # check last migration
    last_migration = db.query(Migration)\
        .select('index', 'tables')\
        .filter(schema=schema)\
        .sort_by('index', desc=True)\
        .fetchone()

    if not last_migration:
        return [MigrationCommand(MigrationType.INITIAL, (None, ))], db.all_tables(schema)

    # load last schema
    tables_old: dict[str, Type[DBTable]] = {}
    data = json.loads(last_migration.tables)
    for chunk in data:
        SomeTable: Type[DBTable] = DBTable._load_schema(chunk)
        tables_old |= {SomeTable.__qualname__: SomeTable}

    globalns = tables_old.copy()
    for t in list(tables_old.values()):
        t.resolve_types(globalns)
    for t in list(tables_old.values()):
        t.resolve_types_many(lambda _: None)

    # get tables from specified module
    all_tables = db.all_tables(schema)

    # extend by related types from other schemas
    for t in all_tables.copy():
        for f in t._fields_.values():
            if f.ref and f.type._schema_ != schema:
                fields = {fname: field for fname, field in f.type._fields_.items() if field.pk or field.cid}
                annotations = {fname: annot for fname, annot in f.type.__annotations__.items() if fname in fields}
                ShortClass = typing.cast(typing.Type[DBTable], type(f.type.__qualname__, (DBTable, ), {
                    '__qualname__': f.type.__qualname__,
                    '__module__': f.type.__module__,
                    '__annotations__': annotations,
                    '_table_': f.type._table_,
                    '_schema_': f.type._schema_,
                    '_extendable_': f.type._extendable_,
                    '_discriminator_': f.type._discriminator_,
                    '_just_for_typing_': True,
                    **fields
                }))
                all_tables.append(ShortClass)

    tables_new = {t.__qualname__: t for t in all_tables}

    # compare two schemes and generate list of changes
    # 1. Check for new tables
    tables_to_add = {name_new: t_new for name_new, t_new in tables_new.items() if name_new not in tables_old and not t_new._just_for_typing_}

    # 2. Check for deleted tables
    tables_to_delete = {name_old: t_old for name_old, t_old in tables_old.items() if name_old not in tables_new and not t_old._just_for_typing_}

    # 3. Check to rename
    tables_to_rename = []
    for pair in rename_list:
        if pair[0] in tables_to_delete and pair[1] in tables_to_add:
            tables_to_rename.append((tables_old._schema_, pair[0], pair[1]))
            del tables_to_delete[pair[0]]
            del tables_to_add[pair[1]]

    # Generate commands
    for name, t in tables_to_add.items():
        commands.append(MigrationCommand(MigrationType.ADD_TABLE, (t, )))
    for name, t in tables_to_delete.items():
        commands.append(MigrationCommand(MigrationType.DELETE_TABLE, (t, )))
    for pair in tables_to_rename:
        commands.append(MigrationCommand(MigrationType.RENAME_TABLE, pair))

    # 4. Check common tables
    tables_old = {name: t for name, t in tables_old.items() if name in tables_new}
    tables_new = {name: t for name, t in tables_new.items() if name in tables_old}

    for t_name, table_old in tables_old.items():
        table_new = tables_new[t_name]

        fields_old = {f.column: f for f in table_old._fields_.values() if not f.prop}
        fields_new = {f.column: f for f in table_new._fields_.values() if not f.prop}
        
        # 4.1. Check new fields
        fields_to_add = {f_name: f for f_name, f in fields_new.items() if f_name not in fields_old}

        # 4.2. Check for deleted fields
        fields_to_delete = {f_name: f for f_name, f in fields_old.items() if f_name not in fields_new}

        # 4.3. Check for renamed fields
        fields_to_rename = []
        for pair in rename_list:
            if pair[0] in fields_to_delete and pair[1] in tables_to_add:
                fields_to_rename.append(pair)
                del fields_to_delete[pair[0]]
                del fields_to_add[pair[1]]

        # Generate commands
        for f in fields_to_add.values():
            commands.append(MigrationCommand(MigrationType.ADD_FIELD, (table_new, f)))
        for f in fields_to_delete.values():
            commands.append(MigrationCommand(MigrationType.DELETE_FIELD, (table_old, f)))
        for pair in fields_to_rename:
            commands.append(MigrationCommand(MigrationType.RENAME_FIELD, (table_new, pair[0], pair[1])))

        # 4.4. Check common fields
        fields_old = {name: f for name, f in fields_old.items() if name in fields_new}
        fields_new = {name: f for name, f in fields_new.items() if name in fields_old}

        for f_name, field_old in fields_old.items():
            field_new = fields_new[f_name]

            # 4.4.1. Check flag changed
            for flag_name in ('pk','cid','prop','required','indexed','unique','default_sql'):
                if getattr(field_old, flag_name) != getattr(field_new, flag_name):
                    commands.append(MigrationCommand(MigrationType.ALTER_FIELD_FLAG, (table_new, field_new, flag_name, getattr(field_new, flag_name))))

            # 4.4.2. Check type changed
            if field_old.type.__name__ != field_new.type.__name__:
                commands.append(MigrationCommand(MigrationType.ALTER_FIELD_TYPE, (table_new, field_new, field_old.type.__name__, field_new.type.__name__)))

    return commands, db.all_tables(schema)


def apply_changes(db: DBFactory, schema: str, commands: list[MigrationCommand], all_tables: list[Type[DBTable]], comments: str = "", debug: bool = False):

    if not commands:
        return

    def save_migration(index: int):
        saved_tables = [t._dump_schema() for t in all_tables]
        json_tables = json.dumps(saved_tables, indent=4)
        saved_commands = [c.save() for c in commands]
        json_commands = json.dumps(saved_commands, indent=4)
        migration = Migration(schema=schema, index=index, tables=json_tables, commands=json_commands, comments=comments)
        db.insert(migration)

        version = MigrationVersion(schema=schema, index=index)
        db.save(version, 'schema')

    if len(commands) == 1 and commands[0].command == MigrationType.INITIAL:
        print("Apply initial migration... ", end='')
        db.create(schema)
        save_migration(1)
        print('Done')
        return

    trans = db._trans
    with db.connection() as conn:
        with conn.transaction():
            for command in commands:
                print(f"Apply command {command}... ", end='')
                match command.command:
                    case MigrationType.ADD_TABLE:
                        conn.execute(trans.create_table(command.subject[0]))
                        for field in command.subject[0]._fields_.values():
                            if field.ref:
                                conn.execute(trans.add_reference(command.subject[0], field))

                    case MigrationType.DELETE_TABLE:
                        for field in command.subject[0]._fields_.values():
                            if field.ref:
                                conn.execute(trans.drop_reference(command.subject[0], field))
                        conn.execute(trans.drop_table(command.subject[0]))

                    case MigrationType.RENAME_TABLE:
                        conn.execute(trans.rename_table(*command.subject))

                    case MigrationType.ADD_FIELD:
                        conn.execute(trans.add_field(*command.subject))
                        if command.subject[1].ref:
                            conn.execute(trans.add_reference(*command.subject))

                    case MigrationType.DELETE_FIELD:
                        if command.subject[1].ref:
                            conn.execute(trans.drop_reference(*command.subject))
                        conn.execute(trans.drop_field(*command.subject))

                    case MigrationType.RENAME_FIELD:
                        conn.execute(trans.rename_field(*command.subject))

                    case MigrationType.ALTER_FIELD_TYPE:
                        conn.execute(trans.alter_field_type(command.subject[0], command.subject[1]))

                    case MigrationType.ALTER_FIELD_FLAG:
                        table, field, flag, value = command.subject
                        match flag:
                            case 'pk':
                                raise QuazyNotSupported
                            case 'cid':
                                raise QuazyNotSupported
                            case 'prop':
                                raise QuazyNotSupported
                            case 'required':
                                if field.ref:
                                    conn.execute(trans.drop_reference(table, field))
                                    conn.execute(trans.add_reference(table, field))
                                else:
                                    if value:
                                        conn.execute(trans.set_not_null(table, field))
                                    else:
                                        conn.execute(trans.drop_not_null(table, field))
                            case 'indexed':
                                if value:
                                    conn.execute(trans.create_index(table, field))
                                else:
                                    conn.execute(trans.drop_index(table, field))
                            case 'unique':
                                if value:
                                    conn.execute(trans.create_index(table, field))
                                else:
                                    conn.execute(trans.drop_index(table, field))
                            case 'default_sql':
                                conn.execute(trans.set_default_value(table, field, value))
                print("Done")

            max_index = db.query(Migration).filter(lambda x: x.schema == schema).fetch_max('index')
            save_migration(max_index+1)

            print("Complete")


def dump_changes(db: DBFactory, schema: str, directory: str):
    import yaml

    migrations: typing.Iterator[Migration] = db.query(Migration).filter(schema=schema)

    for migration in migrations:
        info = '-' + migration.comments[0:32].replace(' ', '-') if migration.comments else ''
        with open(os.path.join(directory, f'{migration.index:04}{info}.yaml'), "wt") as f:
            yaml.dump({
                "comments": migration.comments,
                "commands": json.loads(migration.commands),
                "tables": json.loads(migration.tables),
            }, f)

