from __future__ import annotations

import ast, inspect, textwrap
import typing
from collections.abc import Awaitable
from contextlib import _GeneratorContextManagerBase
from functools import wraps

__all__ = ['make_async', 'hybrid_contextmanager']

class AsyncTransformer(ast.NodeTransformer):
    def __init__(self, func_names):
        self.func_names = func_names

    def should_replace_with_async(self, node):
        if isinstance(node, ast.Name):
            return ast.Name in self.func_names

        if isinstance(node, ast.Attribute):
            return node.attr in self.func_names

        if isinstance(node, ast.Call):
            return self.should_replace_with_async(node.func)

        if isinstance(node, ast.Await):
            return self.should_replace_with_async(node.value)

        return False

    def visit_Call(self, node):
        self.generic_visit(node)

        if self.should_replace_with_async(node.func):
            new_node = ast.Await(value=node)
            return ast.copy_location(new_node, node)

        return node

    def visit_With(self, node: ast.With):
        # Check if any context manager usage needs to be async
        should_convert = False
        for item in node.items:
            if isinstance(item.context_expr, ast.Call):
                if self.should_replace_with_async(item.context_expr.func):
                    should_convert = True
            elif isinstance(item.context_expr, ast.IfExp):
                if self.should_replace_with_async(item.context_expr.body):
                    should_convert = True

        if should_convert:
            new_body = [self.visit(stm) for stm in node.body]
            new_node = ast.AsyncWith(items=node.items, body=new_body)
            return ast.copy_location(new_node, node)

        self.generic_visit(node)
        return node

    def visit_For(self, node: ast.For):
        self.generic_visit(node)

        if self.should_replace_with_async(node.iter):
            new_node = ast.AsyncFor(target=node.target, iter=node.iter, body=node.body, orelse=node.orelse)
            return ast.copy_location(new_node, node)

        return node

    def visit_FunctionDef(self, node):
        self.generic_visit(node)
        new_node = ast.AsyncFunctionDef(
            name=node.name,
            args=node.args,
            body=node.body,
            decorator_list=node.decorator_list,
            returns=node.returns,
            lineno = node.lineno, col_offset = node.col_offset, end_lineno = node.end_lineno, end_col_offset = node.end_col_offset,
        )
        return ast.copy_location(new_node, node)

T = typing.TypeVar('T')
def make_async(func: typing.Callable[..., T], func_names: tuple[str, ...] = ()) -> Awaitable[T] | T:
    source = inspect.getsource(func)
    dedented_source = '\n' * (func.__code__.co_firstlineno-1) + textwrap.dedent(source)
    tree = ast.parse(dedented_source)
    func_def = tree.body[0]

    transformer = AsyncTransformer(func_names)
    new_tree = transformer.visit(tree)
    ast.fix_missing_locations(new_tree)

    code_obj = compile(new_tree, filename=inspect.getfile(func), mode='exec')
    namespace = func.__globals__.copy()
    exec(code_obj, namespace)
    return namespace[func_def.name]

# Merged two classes from `contextlib`
class _HybridGeneratorContextManager(
    _GeneratorContextManagerBase
):
    def __enter__(self):
        del self.args, self.kwds, self.func
        try:
            return next(self.gen)
        except StopIteration:
            raise RuntimeError("generator didn't yield") from None

    def __exit__(self, typ, value, traceback):
        if typ is None:
            try:
                next(self.gen)
            except StopIteration:
                return False
            else:
                raise RuntimeError("generator didn't stop")
        else:
            if value is None:
                value = typ()
            try:
                self.gen.throw(value)
            except StopIteration as exc:
                return exc is not value
            except RuntimeError as exc:
                if exc is value:
                    exc.__traceback__ = traceback
                    return False
                if (
                    isinstance(value, StopIteration)
                    and exc.__cause__ is value
                ):
                    value.__traceback__ = traceback
                    return False
                raise
            except BaseException as exc:
                if exc is not value:
                    raise
                exc.__traceback__ = traceback
                return False
            raise RuntimeError("generator didn't stop after throw()")

    async def __aenter__(self):
        del self.args, self.kwds, self.func
        try:
            return await anext(self.gen)
        except StopAsyncIteration:
            raise RuntimeError("generator didn't yield") from None

    async def __aexit__(self, typ, value, traceback):
        if typ is None:
            try:
                await anext(self.gen)
            except StopAsyncIteration:
                return False
            else:
                raise RuntimeError("generator didn't stop")
        else:
            if value is None:
                value = typ()
            try:
                await self.gen.athrow(value)
            except StopAsyncIteration as exc:
                return exc is not value
            except RuntimeError as exc:
                if exc is value:
                    exc.__traceback__ = traceback
                    return False
                if (
                    isinstance(value, (StopIteration, StopAsyncIteration))
                    and exc.__cause__ is value
                ):
                    value.__traceback__ = traceback
                    return False
                raise
            except BaseException as exc:
                if exc is not value:
                    raise
                exc.__traceback__ = traceback
                return False
            raise RuntimeError("generator didn't stop after a throw()")

def hybrid_contextmanager(func):
    @wraps(func)
    def helper(*args, **kwds):
        return _HybridGeneratorContextManager(func, args, kwds)
    return helper
