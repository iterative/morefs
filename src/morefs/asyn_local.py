import shutil
import sys
from asyncio import get_running_loop, iscoroutinefunction
from contextlib import asynccontextmanager
from functools import partial, wraps
from typing import Awaitable, Callable, TypeVar

import aiofile
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem

if sys.version_info < (3, 10):  # pragma: no cover
    from typing_extensions import ParamSpec
else:  # pragma: no cover
    from typing import ParamSpec

P = ParamSpec("P")
R = TypeVar("R")


def wrap(func: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    @wraps(func)
    async def run(*args: P.args, **kwargs: P.kwargs) -> R:
        loop = get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run


class AsyncLocalFileSystem(AsyncFileSystem, LocalFileSystem):
    mirror_sync_methods = False

    _cat_file = wrap(LocalFileSystem.cat_file)
    _chmod = wrap(LocalFileSystem.chmod)
    _cp_file = wrap(LocalFileSystem.cp_file)
    _created = wrap(LocalFileSystem.created)
    _find_async = wrap(LocalFileSystem.find)
    _get_file_async = wrap(LocalFileSystem.get_file)
    _info = wrap(LocalFileSystem.info)
    _islink = wrap(LocalFileSystem.islink)
    _lexists = wrap(LocalFileSystem.lexists)
    _link = wrap(LocalFileSystem.link)
    _ls = wrap(LocalFileSystem.ls)
    _makedirs = wrap(LocalFileSystem.makedirs)
    _mkdir = wrap(LocalFileSystem.mkdir)
    _modified = wrap(LocalFileSystem.modified)
    _mv_file = wrap(LocalFileSystem.mv_file)
    _pipe_file = wrap(LocalFileSystem.pipe_file)
    _put_file = wrap(LocalFileSystem.put_file)
    _read_bytes = wrap(LocalFileSystem.read_bytes)
    _read_text = wrap(LocalFileSystem.read_text)
    _rm = wrap(LocalFileSystem.rm)
    _rm_file = wrap(LocalFileSystem.rm_file)
    _rmdir = wrap(LocalFileSystem.rmdir)
    _touch = wrap(LocalFileSystem.touch)
    _symlink = wrap(LocalFileSystem.symlink)
    _write_bytes = wrap(LocalFileSystem.write_bytes)
    _write_text = wrap(LocalFileSystem.write_text)
    sign = LocalFileSystem.sign

    async def _get_file(self, src, dst, **kwargs):  # pylint: disable=arguments-renamed
        if not iscoroutinefunction(getattr(dst, "write", None)):
            src = self._strip_protocol(src)
            return await self._get_file_async(src, dst)

        async with self.open_async(src, "rb") as fsrc:
            while True:
                buf = await fsrc.read(length=shutil.COPY_BUFSIZE)
                if not buf:
                    break
                await dst.write(buf)

    @asynccontextmanager
    async def open_async(self, path, mode="rb", **kwargs):
        path = self._strip_protocol(path)
        if self.auto_mkdir and "w" in mode:
            await self._makedirs(self._parent(path), exist_ok=True)

        async with aiofile.async_open(path, mode, **kwargs) as f:
            yield f
