import asyncio
import errno
import os
import posixpath
import shutil

import aiofile
import aiofiles.os
from aiofiles.os import wrap  # type: ignore[attr-defined]
from fsspec.asyn import AbstractAsyncStreamedFile, AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem

aiofiles.os.utime = wrap(os.utime)  # type: ignore[attr-defined]
aiofiles.os.path.islink = wrap(os.path.islink)  # type: ignore[attr-defined]
async_rmtree = wrap(shutil.rmtree)  # type: ignore[attr-defined]
async_copyfile = wrap(shutil.copyfile)  # type: ignore[attr-defined]
async_copy_to_fobj = wrap(LocalFileSystem.get_file)


async def copy_asyncfileobj(fsrc, fdst, length=shutil.COPY_BUFSIZE):
    fsrc_read = fsrc.read
    fdst_write = fdst.write
    while buf := await fsrc_read(length):
        await fdst_write(buf)


# pylint: disable=abstract-method


class AsyncLocalFileSystem(AsyncFileSystem, LocalFileSystem):
    _info = wrap(LocalFileSystem.info)
    _lexists = wrap(LocalFileSystem.lexists)
    _created = wrap(LocalFileSystem.created)
    _modified = wrap(LocalFileSystem.modified)
    _chmod = wrap(LocalFileSystem.chmod)
    _mv_file = wrap(LocalFileSystem.mv_file)
    _makedirs = wrap(LocalFileSystem.makedirs)
    _rmdir = wrap(LocalFileSystem.rmdir)
    _rm_file = wrap(LocalFileSystem.rm_file)

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
        return await async_copy_to_fobj(self, path1, path2)

    async def _cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)
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
        return await aiofiles.os.path.islink(path)

    async def _touch(self, path, **kwargs):
        if await self._exists(path):
            path = self._strip_protocol(path)
            return await aiofiles.os.utime(path, None)
        async with self.open_async(path, "a"):
            pass

    def open_async(
        self, path, mode="rb", **kwargs
    ):  # pylint: disable=invalid-overridden-method
        path = self._strip_protocol(path)
        return aiofile.async_open(path, mode, **kwargs)
