import sys
import inspect
import typing
import ast
from collections import defaultdict

from .db_factory import DBFactory
from .db_table import DBTable
from .db_types import db_type_name, IntEnum, StrEnum, Enum


def gen_stub(db: DBFactory, schema: str = None) -> str:

    def type_name(t) -> str:
        if inspect.isclass(t) and issubclass(t, DBTable):
            return '"'+t.__qualname__+'"'
        if inspect.isclass(t) and issubclass(t, Enum):
            enums.add(t)
            return '"'+t.__qualname__+'"'
        return db_type_name(t)

    def extract_imports(module: str):
        nonlocal imports

        with open(sys.modules[module].__file__, "rt") as f:
            code = f.read()

        tree = ast.parse(code)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports["import"].add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module
                if len(imports[module]) == 1 and '*' in imports[module]:
                    continue
                for alias in node.names:
                    if alias.name == '*':
                        imports[module] = {'*'}
                        break
                    else:
                        imports[module].add(alias.name)

    def imports_list() -> list[str]:
        nonlocal imports

        res = []
        for mod, objs in imports.items():
            if mod.startswith('__'):
                res.append(f'from {mod} import '+', '.join(objs))
        for mod in imports.get('import', []):
            res.append(f'import {mod}')
        for mod, objs in imports.items():
            if mod.startswith('__') or mod == 'import':
                continue
            res.append(f'from {mod} import '+', '.join(objs))

        return res

    def shift(s: str) -> str:
        res = []
        for line in s.splitlines():
            res.append('' if not line else '\t' + line)
        return '\n'.join(res)

    table_chunks = {}
    modules = []
    imports: dict[str, set] = defaultdict(set)
    imports["import"].add("typing")
    enums = set()

    for table in db.all_tables(schema, for_stub=True):
        if (module := table.__module__) not in modules:
            modules.append(module)
            extract_imports(module)

        field_chunks = []
        var_chunks = ['self']
        for name, field in table.DB.fields.items():
            if field.prop:
                field_type = f'{type_name(field.type)}'
            elif field.body:
                field_type = 'dict[str, typing.Any]'
            elif field.cid:
                field_type = f'{type_name(field.type)}'
            elif not field.required:
                field_type = f'{type_name(field.type)} | None'
            else:
                field_type = type_name(field.type)
            field_chunks.append(f'\t{name}: {field_type}')
            var_chunks.append(f'{name}: {type_name(field.type)} = None')

        for name, field in table.DB.many_fields.items():
            field_chunks.append(f'\t{name}: list["{field.source_table.__qualname__}"]')
            var_chunks.append(f'{name}: list["{field.source_table.__qualname__}"] = None')

        for name, field in table.DB.many_to_many_fields.items():
            field_chunks.append(f'\t{name}: list["{field.source_table.__qualname__}"]')
            var_chunks.append(f'{name}: list["{field.source_table.__qualname__}"] = None')

        for name, field in table.DB.subtables.items():
            field_chunks.append(f'\t{name}: list["{field.__qualname__}"]')
            var_chunks.append(f'{name}: list["{field.__qualname__}"] = None')

        init_chunk = f'\n\tdef __init__(' + ', '.join(var_chunks) + '): ...\n'

        table_chunk = f"class {table.__name__}({', '.join(base.__qualname__ for base in table.__bases__)}):\n" + \
            '\n'.join(field_chunks) + \
            init_chunk

        names = table.__qualname__.split('.')
        if len(names) == 1:
            table_chunks[names[0]] = table_chunk
        else:
            table_chunks[names[0]] += '\n' + shift(table_chunk)

    enum_chunks = ['']
    for enum in enums:  # type: typing.Type[IntEnum]
        field_chunks = [f'\t{v.name} = {v.value!r}' for v in enum]
        enum_line = f'class {enum.__name__}({enum.__bases__[0].__name__}):\n' + '\n'.join(field_chunks)
        names = enum.__qualname__.split('.')
        if len(names) == 1:
            enum_chunks.append(enum_line)
        else:
            table_chunks[names[0]] += '\n' + shift(enum_line)

    return '\n'.join(imports_list()) + '\n\n' + '\n\n'.join(table_chunks.values()) + '\n\n'.join(enum_chunks)

