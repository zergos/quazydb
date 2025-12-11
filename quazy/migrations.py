import inspect
import json
import os
import typing
from datetime import datetime
from enum import auto
from typing import NamedTuple, Any

from . import DBFactory, DBTable, DBField
from .db_types import StrEnum, Enum, db_type_by_name
from .exceptions import *

__all__ = ["Migration", "MigrationDifference", "check_migrations", "activate_migrations", "get_migrations_list", "compare_schema", "apply_changes", "clear_migrations"]

_SCHEMA_ = "migrations"


class Migration(DBTable):
    """Inner table to store migrations

    Attributes:
        created_at: datetime - when migration was created
        schema: schema name
        index: migration index
        next_index: next migration index or None
        tables: json with all tables' schema
        commands: json with all commands
        comments: migration comments (human-readable)
        active: is migration active
        reversed: is migration reversed
    """
    created_at: datetime = lambda x: datetime.now()
    schema: str
    index: str
    next_index: str | None
    tables: str
    commands: str
    comments: str
    active: bool = True
    reversed: bool = False

    def __str__(self):
        return '{} {:4s}{} {}'.format(
            '*' if self.active else '-' if self.reversed else ' ',
            self.index,
            f'->{self.next_index}' if self.next_index is not None else '      ',
            self.comments
        )

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
    arguments: tuple[Any, ...]

    def __str__(self):
        match self.command:
            case MigrationType.INITIAL:
                return f"Initial migration"
            case MigrationType.ADD_TABLE:
                return f"Add table `{self.arguments[0].__qualname__}`"
            case MigrationType.DELETE_TABLE:
                return f"Delete table `{self.arguments[0].__qualname__}`"
            case MigrationType.RENAME_TABLE:
                return f"Rename table `{self.arguments[1]}` to `{self.arguments[2]}`"
            case MigrationType.ADD_FIELD:
                return f"Add field `{self.arguments[1].name}` to table `{self.arguments[0].__qualname__}`"
            case MigrationType.DELETE_FIELD:
                return f"Delete field `{self.arguments[1].name}` from table `{self.arguments[0].__qualname__}`"
            case MigrationType.RENAME_FIELD:
                return f"Rename field `{self.arguments[1]}` to `{self.arguments[2]}` at table `{self.arguments[0].__qualname__}`"
            case MigrationType.ALTER_FIELD_TYPE:
                return f"Alter field type `{self.arguments[1].name}` from `{self.arguments[2]}` to `{self.arguments[3]}` at table `{self.arguments[0].__qualname__}`"
            case MigrationType.ALTER_FIELD_FLAG:
                return f"Alter field `{self.arguments[1].name}` flag `{self.arguments[2]}` to value `{self.arguments[3]}` at table `{self.arguments[0].__qualname__}`"
            case _:
                return f"Custom command: `{self.command}` `{self.arguments}`"

    def save(self) -> dict[str, typing.Any]:

        def add_arg(typ: str, val: Any):
            args.append({'type': typ, 'value': val})

        args = []
        if self.arguments:
            for arg in self.arguments:
                if inspect.isclass(arg) and issubclass(arg, DBTable):
                    add_arg('DBTable', arg.__qualname__)
                elif isinstance(arg, DBField):
                    add_arg('DBField', arg.name)
                elif isinstance(arg, str):
                    add_arg('str', arg)
                elif type(arg) is bool:
                    add_arg('bool', str(arg))
                elif arg is None:
                    pass
                else:
                    raise QuazyError('Wrong arg type in command argument')
        return {'command': self.command, 'arguments': args}

    @classmethod
    def load(cls, data: dict[str, typing.Any], tables: dict[str, type[DBTable]]):
        args = []
        for arg in data['arguments']:
            if arg['type'] == 'DBTable':
                args.append(tables[arg['value']])
            elif arg['type'] == 'DBField':
                args.append(args[0].DB.fields[arg['value']])
            elif arg['type'] == 'str':
                args.append(arg['value'])
            elif arg['type'] == 'bool':
                args.append(arg['value'] == 'True')
            else:
                raise QuazyError('Wrong arg type in command loading')
        return cls(command=data['command'], subject=tuple(args))

class MigrationDifference(NamedTuple):
    """Tuple of commands and tables.

    Attributes:
        commands: list of commands
        tables: list of tables
        migration_index: migration index when reverted or None when new migration is created
    """
    commands: list[MigrationCommand]
    tables: list[type[DBTable]]
    migration_index: str | None = None

    def info(self) -> str:
        """Get textual information about the migration difference."""
        result = ''
        if self.migration_index:
            result += f'Migration index: {self.migration_index}\n'
        result += 'Commands:\n'
        result += '\n'.join(str(command) for command in self.commands)
        return result

def check_migrations(db: DBFactory) -> bool:
    """Check if migrations activated"""
    db.bind_module(__name__)
    return db.check(_SCHEMA_)


def activate_migrations(db: DBFactory):
    """Activate migrations.

    This method creates table and schema for migrations.
    """
    db.bind_module(__name__)
    db.create(_SCHEMA_)


def clear_migrations(db: DBFactory, schema: str = None):
    """Clear migrations for the specified schema.

    If `schema` is None, then all migrations are cleared.
    """
    db.bind_module(__name__)
    db.clear(schema or _SCHEMA_)

    if schema:
        db.delete(Migration, filter=lambda x: x.schema == schema)


def get_migrations_list(db: DBFactory, schema: str = 'public') -> list[Migration]:
    """Get a list of migrations for the specified schema."""
    return db.query(Migration).filter(schema=schema).sort_by(lambda x: x.index.as_integer).fetch_all()


def compare_schema(db: DBFactory, rename_list: list[tuple[str, str]] | None = None, migration_index: str | None = None, schema: str = "public") -> MigrationDifference:
    """Compare the last migration with the specified schema.

    Arguments:
        db: database factory
        rename_list: list of tuples of table/field names to rename, like ("old", "new")
        migration_index: migration index to revert to, if `None` then the current schema is compared with the last migration
        schema: schema name (public by default)

    Returns:
        `MigrationDifference` object with a list of commands and tables to apply to the specified schema.
    """
    db.bind_module(__name__)

    commands: list[MigrationCommand] = []

    # check last migration
    last_migration = db.query(Migration)\
        .select('index', 'tables')\
        .filter(schema=schema, active=True)\
        .fetch_one()

    if not last_migration:
        return MigrationDifference([MigrationCommand(MigrationType.INITIAL, (None, ))], db.all_tables(schema))

    if migration_index == last_migration.index:
        raise QuazyError(f'Migration index `{migration_index}` already applied')

    def load_tables(tables_data: str) -> dict[str, type[DBTable]]:
        tables: dict[str, type[DBTable]] = {}
        data = json.loads(tables_data)
        for chunk in data:
            SomeTable: type[DBTable] = DBTable._load_schema(chunk)
            tables |= {SomeTable.__qualname__: SomeTable}

        globalns = tables.copy()
        for t in list(tables.values()):
            t.resolve_types(globalns)
        for t in list(tables.values()):
            t.resolve_types_many(lambda _: None)
        return tables

    # load last schema
    tables_old = load_tables(last_migration.tables)

    if migration_index is None:
        # get tables from the specified module
        all_tables = db.all_tables(schema)
    else:
        # check migration index is within actual branch
        actual_branch = db.query(Migration).select("index").chained("index", "next_index", "0001").fetch_list()
        if migration_index not in actual_branch:
            raise QuazyError(f'Migration index `{migration_index}` is orphaned and can not be reverted anymore')
        # get tables from the specified migration snapshot
        selected_migration = db.query(Migration).select("tables").where(index=migration_index).fetch_one()
        if not selected_migration:
            raise QuazyError(f'No migration index `{migration_index}` found')
        all_tables = list(load_tables(selected_migration.tables).values())

    # extend by related types from other schemas
    for t in all_tables.copy():
        for f in t.DB.fields.values():
            if f.ref and f.type.DB.schema != schema:
                fields = {fname: field for fname, field in f.type.DB.fields.items() if field.pk or field.cid}
                annotations = {fname: annot for fname, annot in f.type.__annotations__.items() if fname in fields}
                ShortClass = typing.cast(type[DBTable], type(f.type.__qualname__, (DBTable, ), {
                    '__qualname__': f.type.__qualname__,
                    '__module__': f.type.__module__,
                    '__annotations__': annotations,
                    '__annotate_func__': lambda f: annotations,
                    '_table_': f.type.DB.table,
                    '_schema_': f.type.DB.schema,
                    '_extendable_': f.type.DB.extendable,
                    '_discriminator_': f.type.DB.discriminator,
                    '_just_for_typing_': True,
                    **fields
                }))
                all_tables.append(ShortClass)

    tables_new = {t.__qualname__: t for t in all_tables}

    # compare two schemes and generate list of changes
    # 1. Check for new tables
    tables_to_add = {name_new: t_new for name_new, t_new in tables_new.items() if name_new not in tables_old and not t_new.DB.just_for_typing}

    # 2. Check for deleted tables
    tables_to_delete = {name_old: t_old for name_old, t_old in tables_old.items() if name_old not in tables_new and not t_old.DB.just_for_typing}

    # 3. Check to rename
    tables_to_rename = []
    if rename_list:
        for pair in rename_list:
            if pair[0] in tables_to_delete and pair[1] in tables_to_add:
                tables_to_rename.append((tables_to_delete[pair[0]].DB.schema, tables_to_delete[pair[0]].DB.table, tables_to_add[pair[1]].DB.table))
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

        fields_old = {f.column: f for f in table_old.DB.fields.values() if not f.property}
        fields_new = {f.column: f for f in table_new.DB.fields.values() if not f.property}
        
        # 4.1. Check new fields
        fields_to_add = {f_name: f for f_name, f in fields_new.items() if f_name not in fields_old}

        # 4.2. Check for deleted fields
        fields_to_delete = {f_name: f for f_name, f in fields_old.items() if f_name not in fields_new}

        # 4.3. Check for renamed fields
        fields_to_rename = []
        if rename_list:
            for pair in rename_list:
                if pair[0] in fields_to_delete and pair[1] in fields_to_add:
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
            if inspect.isclass(field_new.type) and issubclass(field_new.type, Enum):
                field_new.type = db_type_by_name(field_new.type.__base__.__name__)

            # 4.4.1. Check flag changed
            for flag_name in ('pk','cid','property','required','indexed','unique','default_sql'):
                if getattr(field_old, flag_name) != getattr(field_new, flag_name):
                    commands.append(MigrationCommand(MigrationType.ALTER_FIELD_FLAG, (table_new, field_new, flag_name, getattr(field_new, flag_name))))

            # 4.4.2. Check type changed
            if field_old.type.__name__ != field_new.type.__name__:
                commands.append(MigrationCommand(MigrationType.ALTER_FIELD_TYPE, (table_new, field_new, field_old.type.__name__, field_new.type.__name__)))

    return MigrationDifference(commands, db.all_tables(schema), migration_index)


def apply_changes(db: DBFactory, diff: MigrationDifference, comments: str = "", schema: str = 'public'):
    """Apply changes from the specified migration difference.

    Arguments:
        db: database factory
        diff: migration difference
        comments: optional comments for the migration (human-readable)
        schema: schema name (public by default)
    """

    if not diff.commands:
        return

    def save_migration(index: str):
        saved_tables = [t._dump_schema() for t in diff.tables]
        json_tables = json.dumps(saved_tables, indent=4)
        saved_commands = [c.save() for c in diff.commands]
        json_commands = json.dumps(saved_commands, indent=4)
        migration = Migration(schema=schema, index=index, tables=json_tables, commands=json_commands, comments=comments)
        db.insert(migration)

    if len(diff.commands) == 1 and diff.commands[0].command == MigrationType.INITIAL:
        print("Apply initial migration... ", end='')
        db.create(schema)
        save_migration('0001')
        print('Done')
        return

    trans = db._translator
    with db.connection() as conn:
        for command in diff.commands:
            print(f"Apply command: {command}... ", end='')
            match command.command:
                case MigrationType.ADD_TABLE:
                    conn.execute(trans.create_table(command.arguments[0]))
                    for field in command.arguments[0].DB.fields.values():
                        if field.ref:
                            conn.execute(trans.add_reference(command.arguments[0], field))

                case MigrationType.DELETE_TABLE:
                    for field in command.arguments[0].DB.fields.values():
                        if field.ref:
                            conn.execute(trans.drop_reference(command.arguments[0], field))
                    conn.execute(trans.drop_table(command.arguments[0]))

                case MigrationType.RENAME_TABLE:
                    conn.execute(trans.rename_table(*command.arguments))

                case MigrationType.ADD_FIELD:
                    conn.execute(trans.add_field(*command.arguments))
                    if command.arguments[1].ref:
                        conn.execute(trans.add_reference(*command.arguments))

                case MigrationType.DELETE_FIELD:
                    if command.arguments[1].ref:
                        conn.execute(trans.drop_reference(*command.arguments))
                    conn.execute(trans.drop_field(*command.arguments))

                case MigrationType.RENAME_FIELD:
                    conn.execute(trans.rename_field(*command.arguments))

                case MigrationType.ALTER_FIELD_TYPE:
                    conn.execute(trans.alter_field_type(command.arguments[0], command.arguments[1]))

                case MigrationType.ALTER_FIELD_FLAG:
                    table, field, flag, value = command.arguments
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

    # set migration statuses
    last_mig = db.get(Migration, active=True)
    if diff.migration_index is None:
        max_index = db.query(Migration).filter(lambda x: x.schema == schema).fetch_max(lambda x: x.index.as_integer)
        next_index = f'{max_index+1:04d}'
        save_migration(next_index)
        if last_mig:
            last_mig.active = False
            last_mig.next_index = next_index
            last_mig.save()
    else:
        if int(diff.migration_index) < int(last_mig.index):
            q = db.query(Migration).chained("index", "next_index", diff.migration_index)
            for x in q:
                if x.index > diff.migration_index:
                    x.active = False
                    x.reversed = True
                else:
                    x.active = True
                x.save()
                if x.index == last_mig.index:
                    break
        else:
            q = db.query(Migration).chained("index", "next_index", last_mig.index)
            for x in q:
                x.reversed = False
                if x.index == diff.migration_index:
                    x.active = True
                    x.save()
                    break
                else:
                    x.active = False
                x.save()

    print("Complete")


def dump_changes(db: DBFactory, schema: str, directory: str):
    """Dump changes for the specified schema to the specified directory in YAML format."""

    import yaml

    migrations = db.query(Migration).chained("index", "next_index", "0001").filter(schema=schema)

    for migration in migrations:
        info = '-' + migration.comments[0:32].replace(' ', '-') if migration.comments else ''
        with open(os.path.join(directory, f'{migration.index}{info}.yaml'), "wt") as f:
            yaml.dump({
                "comments": migration.comments,
                "commands": json.loads(migration.commands),
                "tables": json.loads(migration.tables),
            }, f, sort_keys=False)

