import asyncio
import datetime
import errno
import os
import shutil
import stat

import aiofile
import aiofiles.os
from aiofiles.os import wrap  # type: ignore[attr-defined]
from aiopath.scandir import EntryWrapper, scandir_async
from fsspec.asyn import AbstractBufferedFile, AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem

aiofiles.os.listdir = wrap(os.listdir)  # type: ignore[attr-defined]
aiofiles.os.link = wrap(os.link)  # type: ignore[attr-defined]
aiofiles.os.symlink = wrap(os.symlink)  # type: ignore[attr-defined]
aiofiles.os.islink = wrap(os.path.islink)  # type: ignore[attr-defined]
aiofiles.os.readlink = wrap(os.readlink)  # type: ignore[attr-defined]
aiofiles.os.scandir = wrap(os.scandir)  # type: ignore[attr-defined]
aiofiles.os.lexists = wrap(os.path.lexists)  # type: ignore[attr-defined]
aiofiles.os.chmod = wrap(os.chmod)  # type: ignore[attr-defined]
aiofiles.os.utime = wrap(os.utime)  # type: ignore[attr-defined]
async_rmtree = wrap(shutil.rmtree)  # type: ignore[attr-defined]
async_move = wrap(shutil.move)  # type: ignore[attr-defined]
async_copyfile = wrap(shutil.copyfile)  # type: ignore[attr-defined]


def _copy_to_fobj(fs, path1, fdst):
    with fs.open(path1, "rb") as fsrc:
        shutil.copyfileobj(fsrc, fdst)


async_copy_to_fobj = wrap(_copy_to_fobj)


async def copy_asyncfileobj(fsrc, fdst, length=shutil.COPY_BUFSIZE):
    fsrc_read = fsrc.read
    fdst_write = fdst.write
    while buf := await fsrc_read(length):
        await fdst_write(buf)


# pylint: disable=arguments-renamed


class AsyncLocalFileSystem(AsyncFileSystem):  # pylint: disable=abstract-method
    async def _info(self, path, **kwargs):
        if isinstance(path, os.DirEntry):
            path = EntryWrapper(path)
        if isinstance(path, EntryWrapper):
            out = await path.stat(follow_symlinks=False)
            link = await path.is_symlink()
            if await path.is_dir(follow_symlinks=False):
                t = "directory"
            elif await path.is_file(follow_symlinks=False):
                t = "file"
            else:
                t = "other"
            path = path.path
        else:
            out = await aiofiles.os.stat(path, follow_symlinks=False)
            link = stat.S_ISLNK(out.st_mode)
            if link:
                out = await aiofiles.os.stat(path, follow_symlinks=True)
            if stat.S_ISDIR(out.st_mode):
                t = "directory"
            elif stat.S_ISREG(out.st_mode):
                t = "file"
            else:
                t = "other"
        result = {
            "name": path,
            "size": out.st_size,
            "type": t,
            "created": out.st_ctime,
            "islink": link,
        }
        for field in ["mode", "uid", "gid", "mtime"]:
            result[field] = getattr(out, "st_" + field)
        if result["islink"]:
            result["destination"] = await aiofiles.os.readlink(path)
            try:
                out2 = await aiofiles.os.stat(path, follow_symlinks=True)
                result["size"] = out2.st_size
            except IOError:
                result["size"] = 0
        return result

    async def _ls(self, path, detail=True, **kwargs):
        if detail:
            return [await self._info(f) async for f in scandir_async(path)]
        return [os.path.join(path, f) for f in await aiofiles.os.listdir(path)]

    async def _rm_file(self, path, **kwargs):
        await aiofiles.os.remove(path)

    async def _rmdir(self, path):
        await aiofiles.os.rmdir(path)

    async def _mkdir(self, path, create_parents=True, **kwargs):
        if create_parents:
            if await self._exists(path):
                raise FileExistsError(
                    errno.EEXIST, os.strerror(errno.EEXIST), path
                )
            return await self._makedirs(path, exist_ok=True)
        await aiofiles.os.mkdir(path)

    async def _makedirs(self, path, exist_ok=False):
        await aiofiles.os.makedirs(path, exist_ok=exist_ok)

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
        with self.open_async(path, "wb") as f:
            await f.write(value)

    async def _put_file(self, path1, path2, **kwargs):
        await self._cp_file(path1, path2, **kwargs)

    async def _get_file(self, path1, path2, **kwargs):
        write_method = getattr(path2, "write", None)
        if not write_method:
            return await self._cp_file(path1, path2, **kwargs)
        if isinstance(
            path2, AbstractBufferedFile
        ) or asyncio.iscoroutinefunction(write_method):
            with self.open_async(path1, "rb") as fsrc:
                return await async_copy_to_fobj(fsrc, path2)
        return await async_copy_to_fobj(path1, path2)

    async def _cp_file(self, path1, path2, **kwargs):
        if await self._isfile(path1):
            return await async_copyfile(path1, path2)
        if await self._isdir(path1):
            return await self._makedirs(path2, exist_ok=True)
        raise FileNotFoundError

    async def _mv_file(self, path1, path2, **kwargs):
        await async_move(path1, path2)

    async def _lexists(self, path, **kwargs):
        return await aiofiles.os.lexists(path)

    async def _created(self, path):
        info = await self._info(path=path)
        return datetime.datetime.utcfromtimestamp(info["created"])

    async def _modified(self, path):
        info = await self.info(path=path)
        return datetime.datetime.utcfromtimestamp(info["mtime"])

    async def _rm(
        self, path, recursive=False, maxdepth=None
    ):  # pylint: disable=arguments-differ, unused-argument
        if isinstance(path, str):
            path = [path]

        for p in path:
            if recursive and await self._isdir(p):
                if os.path.abspath(p) == os.getcwd():
                    raise ValueError("Cannot delete current working directory")
                await async_rmtree(p)
            else:
                await aiofiles.os.remove(p)

    async def _chmod(self, path, mode):
        await aiofiles.os.chmod(path, mode)

    async def _link(self, src, dst):
        await aiofiles.os.link(src, dst)

    async def _symlink(self, src, dst):
        await aiofiles.os.symlink(src, dst)

    async def _islink(self, path):
        return await aiofiles.os.islink(path)

    async def _touch(self, path, **kwargs):
        if self._exists(path):
            return await aiofiles.os.utime(path, None)
        async with self.open_async(path, "a"):
            pass

    _open = LocalFileSystem._open  # pylint: disable=protected-access

    def open_async(  # pylint: disable=invalid-overridden-method
        self, path, mode="rb", **kwargs
    ):
        return aiofile.async_open(path, mode, **kwargs)
