from os import fspath

import pytest
from fsspec.implementations.local import LocalFileSystem
from morefs.asyn_local import AsyncLocalFileSystem


@pytest.fixture
def fs():
    return AsyncLocalFileSystem()


@pytest.fixture
def localfs():
    return LocalFileSystem()


@pytest.mark.asyncio
async def test_ls(tmp_path, localfs, fs):
    struct = {
        fspath(tmp_path / "foo"): b"foo",
        fspath(tmp_path / "bar"): b"bar",
        fspath(tmp_path / "dir" / "file"): b"file",
    }
    localfs.mkdir(tmp_path / "dir")
    localfs.pipe(struct)

    assert set(await fs._ls(tmp_path, detail=False)) == {
        localfs._strip_protocol(tmp_path / f) for f in ["foo", "bar", "dir"]
    }
    assert await fs._ls(tmp_path, detail=False) == localfs.ls(tmp_path, detail=False)

    assert await fs._info(tmp_path / "foo") == localfs.info(tmp_path / "foo")
    assert await fs._info(tmp_path / "dir") == localfs.info(tmp_path / "dir")

    assert await fs._ls(tmp_path, detail=True) == localfs.ls(tmp_path, detail=True)

    assert await fs._find(tmp_path, detail=False) == localfs.find(
        tmp_path,
        detail=False,
    )
    assert await fs._find(tmp_path, detail=True) == localfs.find(tmp_path, detail=True)

    assert await fs._isfile(tmp_path / "foo")
    assert await fs._isdir(tmp_path / "dir")
    assert await fs._exists(tmp_path / "bar")
    assert not await fs._exists(tmp_path / "not-existing-file")
    assert await fs._lexists(tmp_path / "foo")


def test_sync_methods(tmp_path, localfs, fs):
    struct = {
        fspath(tmp_path / "foo"): b"foo",
        fspath(tmp_path / "bar"): b"bar",
        fspath(tmp_path / "dir" / "file"): b"file",
    }
    localfs.mkdir(tmp_path / "dir")
    localfs.pipe(struct)

    assert set(fs.ls(tmp_path, detail=False)) == {
        localfs._strip_protocol(tmp_path / f) for f in ["foo", "bar", "dir"]
    }
    assert fs.ls(tmp_path, detail=False) == localfs.ls(tmp_path, detail=False)

    assert fs.info(tmp_path / "foo") == localfs.info(tmp_path / "foo")
    assert fs.info(tmp_path / "dir") == localfs.info(tmp_path / "dir")

    assert fs.ls(tmp_path, detail=True) == localfs.ls(tmp_path, detail=True)
    assert fs.find(tmp_path, detail=False) == localfs.find(tmp_path, detail=False)
    assert fs.find(tmp_path, detail=True) == localfs.find(tmp_path, detail=True)

    assert fs.isfile(tmp_path / "foo")
    assert fs.isdir(tmp_path / "dir")
    assert fs.exists(tmp_path / "bar")
    assert not fs.exists(tmp_path / "not-existing-file")
    assert fs.lexists(tmp_path / "foo")


@pytest.mark.asyncio
async def test_open_async(tmp_path, fs):
    f = await fs.open_async(tmp_path / "file", mode="wb")
    async with f:
        pass
    assert await fs._exists(tmp_path / "file")

    f = await fs.open_async(tmp_path / "file", mode="wb")
    async with f:
        assert await f.write(b"contents")

    f = await fs.open_async(tmp_path / "file")
    async with f:
        assert await f.read() == b"contents"


@pytest.mark.asyncio
async def test_get_file(tmp_path, fs):
    await fs._pipe_file(tmp_path / "foo", b"foo")
    await fs._get_file(tmp_path / "foo", tmp_path / "bar")

    assert await fs._isfile(tmp_path / "bar")

    f = await fs.open_async(tmp_path / "file1", mode="wb")
    async with f:
        await fs._get_file(tmp_path / "foo", f)
    assert await fs._cat_file(tmp_path / "file1") == b"foo"

    with fs.open(tmp_path / "file2", mode="wb") as f:
        await fs._get_file(tmp_path / "foo", f)
    assert await fs._cat_file(tmp_path / "file2") == b"foo"

    with (tmp_path / "file3").open(mode="wb") as f:
        await fs._get_file(tmp_path / "foo", f)
    assert await fs._cat_file(tmp_path / "file3") == b"foo"


@pytest.mark.asyncio
async def test_auto_mkdir_on_open_async(tmp_path):
    fs = AsyncLocalFileSystem(auto_mkdir=True)
    f = await fs.open_async(tmp_path / "dir" / "file", mode="wb")
    async with f:
        await f.write(b"contents")

    assert await fs._isdir(tmp_path / "dir")
    assert await fs._isfile(tmp_path / "dir" / "file")
    assert await fs._cat_file(tmp_path / "dir" / "file") == b"contents"
