import asyncio
import inspect

from asyncpg import connect as connect_async, Connection as ConnectionAsync, Record
from asyncpg.pool import Pool as PoolAsync
from asyncpg import exceptions

import typing

__all__ = ('connect', 'create_pool', 'Record', 'Connection') + \
          exceptions.__all__  # NOQA


def func_sync(func: typing.Callable) -> typing.Callable:
    def wrapper(*args, **kwargs):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        if loop.is_running():
            return func(*args, **kwargs)
        else:
            return loop.run_until_complete(func(*args, **kwargs))
    return wrapper


def class_sync(cls: typing.ClassVar) -> typing.ClassVar:
    for name, func in cls.__bases__[0].__dict__.items():  # type: str, typing.Any
        if inspect.isfunction(func) and func.__code__.co_flags & 0x80:
            if name == '__aenter__':
                setattr(cls, '__enter__', func_sync(func))
            elif name == '__aexit__':
                setattr(cls, '__exit__', func_sync(func))
            elif name == '__await__':
                setattr(cls, 'get_object', func_sync(func))
            setattr(cls, name, func_sync(func))
    return cls


connect_sync = func_sync(connect_async)


def connect(dsn=None, *,
            host=None, port=None,
            user=None, password=None, passfile=None,
            database=None,
            loop=None,
            timeout=60,
            statement_cache_size=100,
            max_cached_statement_lifetime=300,
            max_cacheable_statement_size=1024 * 15,
            command_timeout=None,
            ssl=None,
            server_settings=None) -> 'Connection':
    return connect_sync(dsn=dsn,
                        connection_class=Connection,
                        host=host, port=port,
                        user=user, password=password, passfile=passfile,
                        database=database,
                        loop=loop,
                        timeout=timeout,
                        statement_cache_size=statement_cache_size,
                        max_cached_statement_lifetime=max_cached_statement_lifetime,
                        max_cacheable_statement_size=max_cacheable_statement_size,
                        command_timeout=command_timeout,
                        ssl=ssl,
                        server_settings=server_settings)


def create_pool(dsn=None, *,
                min_size=10,
                max_size=10,
                max_queries=50000,
                max_inactive_connection_lifetime=300.0,
                setup=None,
                init=None,
                loop=None,
                **connect_kwargs) -> 'Pool':
    return Pool(dsn=dsn,
                connection_class=Connection,
                min_size=min_size,
                max_size=max_size,
                max_queries=max_queries,
                max_inactive_connection_lifetime=max_inactive_connection_lifetime,
                setup=setup,
                init=init,
                loop=loop,
                **connect_kwargs)


@class_sync
class Connection(ConnectionAsync):
    __slots__ = ()


@class_sync
class Pool(PoolAsync):
    __slots__ = ()
