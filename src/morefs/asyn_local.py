import asyncio
import errno
import os
import posixpath
import shutil
from contextlib import asynccontextmanager

import aiofile
import aiofiles.os
from aiofiles.os import wrap  # type: ignore[attr-defined]
from fsspec.asyn import AbstractAsyncStreamedFile, AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem

async_utime = wrap(os.utime)
async_islink = wrap(os.path.islink)
async_rmtree = wrap(shutil.rmtree)
async_copyfile = wrap(shutil.copyfile)
async_get_file = wrap(LocalFileSystem.get_file)


async def copy_asyncfileobj(fsrc, fdst, length=shutil.COPY_BUFSIZE):
    fsrc_read = fsrc.read
    fdst_write = fdst.write
    while buf := await fsrc_read(length):
        await fdst_write(buf)


# pylint: disable=abstract-method


class AsyncLocalFileSystem(AsyncFileSystem, LocalFileSystem):
    _chmod = wrap(LocalFileSystem.chmod)
    _created = wrap(LocalFileSystem.created)
    _info = wrap(LocalFileSystem.info)
    _lexists = wrap(LocalFileSystem.lexists)
    _makedirs = wrap(LocalFileSystem.makedirs)
    _modified = wrap(LocalFileSystem.modified)
    _mv_file = wrap(LocalFileSystem.mv_file)
    _rm_file = wrap(LocalFileSystem.rm_file)
    _rmdir = wrap(LocalFileSystem.rmdir)

    async def _ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        if detail:
            with await aiofiles.os.scandir(path) as entries:
                return [await self._info(f) for f in entries]
        return [
            posixpath.join(path, f) for f in await aiofiles.os.listdir(path)
        ]

    async def _mkdir(self, path, create_parents=True, **kwargs):
        if create_parents:
            if await self._exists(path):
                raise FileExistsError(
                    errno.EEXIST, os.strerror(errno.EEXIST), path
                )
            return await self._makedirs(path, exist_ok=True)
        path = self._strip_protocol(path)
        await aiofiles.os.mkdir(path)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        async with self.open_async(path, "rb") as f:
            if start is not None:
                if start >= 0:
                    f.seek(start)
                else:
                    f.seek(max(0, f.size + start))
            if end is not None:
                if end < 0:
                    end = f.size + end
                return await f.read(end - f.tell())
            return await f.read()

    async def _pipe_file(self, path, value, **kwargs):
        async with self.open_async(path, "wb") as f:
            await f.write(value)

    async def _get_file(  # pylint: disable=arguments-renamed
        self, path1, path2, **kwargs
    ):
        write_method = getattr(path2, "write", None)
        if not write_method:
            return await self._cp_file(path1, path2, **kwargs)
        if isinstance(
            path2, AbstractAsyncStreamedFile
        ) or asyncio.iscoroutinefunction(write_method):
            async with self.open_async(path1, "rb") as fsrc:
                return await copy_asyncfileobj(fsrc, path2)

        path1 = self._strip_protocol(path1)
        return await async_get_file(self, path1, path2)

    async def _cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
        if self.auto_mkdir:
            await self._makedirs(self._parent(path2), exist_ok=True)
        if await self._isfile(path1):
            return await async_copyfile(path1, path2)
        if await self._isdir(path1):
            return await self._makedirs(path2, exist_ok=True)
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path1)

    _put_file = _cp_file

    async def _rm(
        self, path, recursive=False, batch_size=None, maxdepth=None, **kwargs
    ):
        if isinstance(path, (str, os.PathLike)):
            path = [path]

        assert not maxdepth and not batch_size
        for p in path:
            p = self._strip_protocol(p)
            if recursive and await self._isdir(p):
                if os.path.abspath(p) == os.getcwd():
                    raise ValueError("Cannot delete current working directory")
                await async_rmtree(p)
            else:
                await aiofiles.os.remove(p)

    async def _link(self, src, dst):
        src = self._strip_protocol(src)
        dst = self._strip_protocol(dst)
        await aiofiles.os.link(src, dst)

    async def _symlink(self, src, dst):
        src = self._strip_protocol(src)
        dst = self._strip_protocol(dst)
        await aiofiles.os.symlink(src, dst)

    async def _islink(self, path):
        path = self._strip_protocol(path)
        return await async_islink(path)

    async def _touch(self, path, **kwargs):
        if self.auto_mkdir:
            await self._makedirs(self._parent(path), exist_ok=True)
        if await self._exists(path):
            path = self._strip_protocol(path)
            return await async_utime(path, None)
        async with self.open_async(path, "a"):
            pass

    @asynccontextmanager
    async def open_async(self, path, mode="rb", **kwargs):
        path = self._strip_protocol(path)
        if self.auto_mkdir and "w" in mode:
            await self._makedirs(self._parent(path), exist_ok=True)

        async with aiofile.async_open(path, mode, **kwargs) as f:
            yield f
