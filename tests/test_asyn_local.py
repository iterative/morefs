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
        fspath(tmp_path / f) for f in ["foo", "bar", "dir"]
    }
    assert await fs._ls(tmp_path, detail=False) == localfs.ls(
        tmp_path, detail=False
    )

    assert await fs._info(tmp_path / "foo") == localfs.info(tmp_path / "foo")
    assert await fs._info(tmp_path / "dir") == localfs.info(tmp_path / "dir")

    assert await fs._ls(tmp_path, detail=True) == localfs.ls(
        tmp_path, detail=True
    )

    assert await fs._find(tmp_path, detail=False) == localfs.find(
        tmp_path, detail=False
    )
    assert await fs._find(tmp_path, detail=True) == localfs.find(
        tmp_path, detail=True
    )

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
        fspath(tmp_path / f) for f in ["foo", "bar", "dir"]
    }
    assert fs.ls(tmp_path, detail=False) == localfs.ls(tmp_path, detail=False)

    assert fs.info(tmp_path / "foo") == localfs.info(tmp_path / "foo")
    assert fs.info(tmp_path / "dir") == localfs.info(tmp_path / "dir")

    assert fs.ls(tmp_path, detail=True) == localfs.ls(tmp_path, detail=True)
    assert fs.find(tmp_path, detail=False) == localfs.find(
        tmp_path, detail=False
    )
    assert fs.find(tmp_path, detail=True) == localfs.find(
        tmp_path, detail=True
    )

    assert fs.isfile(tmp_path / "foo")
    assert fs.isdir(tmp_path / "dir")
    assert fs.exists(tmp_path / "bar")
    assert not fs.exists(tmp_path / "not-existing-file")
    assert fs.lexists(tmp_path / "foo")


@pytest.mark.asyncio
async def test_mkdir(tmp_path, fs):
    await fs._mkdir(tmp_path / "dir", create_parents=False)
    assert await fs._isdir(tmp_path / "dir")

    await fs._mkdir(tmp_path / "dir2" / "sub")
    assert await fs._isdir(tmp_path / "dir2")
    assert await fs._isdir(tmp_path / "dir2" / "sub")


@pytest.mark.asyncio
async def test_mkdir_twice_failed(tmp_path, fs):
    await fs._mkdir(tmp_path / "dir")
    with pytest.raises(FileExistsError):
        await fs._mkdir(tmp_path / "dir")


@pytest.mark.asyncio
async def test_open_async(tmp_path, fs):
    async with fs.open_async(tmp_path / "file", mode="wb") as f:
        pass
    assert await fs._exists(tmp_path / "file")

    async with fs.open_async(tmp_path / "file", mode="wb") as f:
        assert await f.write(b"contents")

    async with fs.open_async(tmp_path / "file") as f:
        assert await f.read() == b"contents"


@pytest.mark.asyncio
async def test_pipe_cat_put(tmp_path, fs):
    value = b"foo" * 1000
    await fs._pipe_file(tmp_path / "foo", value)
    assert await fs._cat_file(tmp_path / "foo") == value
    assert await fs._cat_file(tmp_path / "foo", start=100) == value[100:]
    assert (
        await fs._cat_file(tmp_path / "foo", start=100, end=1000)
        == value[100:1000]
    )

    await fs._put_file(tmp_path / "foo", tmp_path / "bar")
    assert await fs._isfile(tmp_path / "bar")


@pytest.mark.asyncio
async def test_cp_file(tmp_path, fs):
    await fs._mkdir(tmp_path / "dir")
    await fs._cp_file(tmp_path / "dir", tmp_path / "dir2")
    assert await fs._isdir(tmp_path / "dir")

    await fs._pipe_file(tmp_path / "foo", b"foo")
    assert await fs._cp_file(tmp_path / "foo", tmp_path / "bar")
    assert await fs._cat_file(tmp_path / "bar") == b"foo"


@pytest.mark.asyncio
async def test_get_file(tmp_path, fs):
    await fs._pipe_file(tmp_path / "foo", b"foo")
    await fs._get_file(tmp_path / "foo", tmp_path / "bar")

    assert await fs._isfile(tmp_path / "bar")

    async with fs.open_async(tmp_path / "file1", mode="wb") as f:
        await fs._get_file(tmp_path / "foo", f)
    assert await fs._cat_file(tmp_path / "file1") == b"foo"

    with fs.open(tmp_path / "file2", mode="wb") as f:
        await fs._get_file(tmp_path / "foo", f)
    assert await fs._cat_file(tmp_path / "file2") == b"foo"

    with (tmp_path / "file3").open(mode="wb") as f:
        await fs._get_file(tmp_path / "foo", f)
    assert await fs._cat_file(tmp_path / "file3") == b"foo"


@pytest.mark.asyncio
async def test_rm(tmp_path, fs):
    await fs._pipe_file(tmp_path / "foo", b"foo")
    await fs._pipe_file(tmp_path / "bar", b"bar")
    await fs._mkdir(tmp_path / "dir")
    await fs._pipe_file(tmp_path / "dir" / "file", b"file")

    await fs._rm_file(tmp_path / "foo")
    assert not await fs._exists(tmp_path / "foo")

    await fs._rm(tmp_path / "bar")
    assert not await fs._exists(tmp_path / "bar")

    with pytest.raises(IsADirectoryError):
        await fs._rm(tmp_path / "dir")

    await fs._rm(tmp_path / "dir", recursive=True)
    assert not await fs._exists(tmp_path / "dir")


@pytest.mark.asyncio
async def test_try_rm_recursive_cwd(tmp_path, monkeypatch, fs):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        await fs._rm(tmp_path, recursive=True)


@pytest.mark.asyncio
async def test_link(tmp_path, fs):
    fs.pipe_file(tmp_path / "foo", b"foo")
    await fs._link(tmp_path / "foo", tmp_path / "foo_link")
    assert (tmp_path / "foo_link").stat().st_nlink > 1


@pytest.mark.asyncio
async def test_symlink(tmp_path, fs):
    fs.pipe_file(tmp_path / "foo", b"foo")
    await fs._symlink(tmp_path / "foo", tmp_path / "foo_link")
    assert await fs._islink(tmp_path / "foo_link")


@pytest.mark.asyncio
async def test_touch(tmp_path, fs):
    await fs._touch(tmp_path / "file")
    assert await fs._exists(tmp_path / "file")
    created = await fs._created(tmp_path / "file")
    await fs._touch(tmp_path / "file")
    assert await fs._modified(tmp_path / "file") >= created
