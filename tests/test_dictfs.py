import errno
from unittest.mock import ANY

import pytest
from morefs.dict import DictFS


@pytest.fixture
def dfs():
    return DictFS()


def test_dictfs_should_not_be_cached():
    assert DictFS() is not DictFS()


def test_strip(dfs):
    assert dfs._strip_protocol("") == ""
    assert dfs._strip_protocol("/") == ""
    assert dfs._strip_protocol("dictfs://") == ""
    assert dfs._strip_protocol("afile") == "/afile"
    assert dfs._strip_protocol("dir/afile") == "/dir/afile"
    assert dfs._strip_protocol("/b/c") == "/b/c"
    assert dfs._strip_protocol("/b/c/") == "/b/c"


def test_info(dfs):
    dfs.mkdir("/dir")
    dfs.pipe_file("/dir/file", b"contents")

    assert dfs.info("/") == {"name": "", "size": 0, "type": "directory"}
    assert dfs.info("") == {"name": "", "size": 0, "type": "directory"}
    assert dfs.info("/dir") == {"name": "/dir", "size": 0, "type": "directory"}
    assert dfs.info("/dir/file") == {
        "name": "/dir/file",
        "size": 8,
        "type": "file",
        "created": ANY,
    }


def test_info_errors(dfs):
    dfs.touch("/afile")
    with pytest.raises(NotADirectoryError):
        dfs.info("/afile/foo")

    with pytest.raises(FileNotFoundError):
        dfs.info("/not-existing")


def test_ls(dfs):
    dfs.makedirs("/dir/dir1")
    dfs.touch("/dir/afile")
    dfs.touch("/dir/dir1/bfile")
    dfs.touch("/dir/dir1/cfile")

    assert dfs.ls("/", False) == ["/dir"]
    assert dfs.ls("/dir", False) == ["/dir/dir1", "/dir/afile"]
    assert dfs.ls("/dir", True)[0]["type"] == "directory"
    assert dfs.ls("/dir", True)[1]["type"] == "file"

    assert len(dfs.ls("/dir/dir1")) == 2
    assert dfs.ls("/dir/afile") == ["/dir/afile"]
    assert dfs.ls("/dir/dir1/bfile") == ["/dir/dir1/bfile"]
    assert dfs.ls("/dir/dir1/cfile") == ["/dir/dir1/cfile"]

    assert dfs.ls("/dir/afile", True)[0] == {
        "name": "/dir/afile",
        "type": "file",
        "size": 0,
        "created": ANY,
    }
    with pytest.raises(FileNotFoundError):
        dfs.ls("/dir/not-existing-file")

    with pytest.raises(NotADirectoryError):
        dfs.ls("/dir/afile/foo")


def test_rm_file(dfs):
    dfs.touch("/afile")
    dfs.rm_file("/afile")
    assert not dfs.exists("/afile")


def test_try_rm_file_not_existing(dfs):
    with pytest.raises(FileNotFoundError):
        dfs.rm_file("/not-existing")


def test_try_rm_file_directory(dfs):
    dfs.mkdir("dir")
    with pytest.raises(IsADirectoryError):
        dfs.rm_file("/dir")

    with pytest.raises(FileNotFoundError):
        dfs.rm_file("/dir/file")
    assert dfs.isdir("/dir")


def test_try_rm_file_under_filepath(dfs):
    dfs.mkdir("/dir")
    dfs.touch("/dir/file")
    with pytest.raises(NotADirectoryError):
        dfs.rm_file("/dir/file/foo")


def test_rmdir(dfs):
    dfs.mkdir("/dir")
    dfs.rmdir("/dir")
    assert not dfs.exists("/dir")


def test_try_rmdir_not_existing(dfs):
    with pytest.raises(FileNotFoundError):
        dfs.rmdir("/dir")


def test_try_rmdir_file(dfs):
    dfs.touch("/afile")
    with pytest.raises(NotADirectoryError):
        dfs.rmdir("/afile")
    assert dfs.exists("/afile")


def test_try_rmdir_non_empty_directory(dfs):
    dfs.mkdir("/dir")
    dfs.touch("/dir/afile")
    with pytest.raises(OSError) as exc:  # noqa: PT011
        dfs.rmdir("/dir")
    assert exc.value.errno == errno.ENOTEMPTY


def test_try_rmdir_under_filepath(dfs):
    dfs.mkdir("/dir")
    dfs.touch("/dir/file")
    with pytest.raises(NotADirectoryError):
        dfs.rmdir("/dir/file/foo")


def test_rm_multiple_files(dfs):
    dfs.mkdir("/dir")
    dfs.touch("/dir/file1")
    dfs.touch("/dir/file2")

    dfs.rm(["/dir/file1", "/dir/file2", "/dir", "/"])
    assert not dfs.ls("/")


def test_remove_all(dfs):
    dfs.touch("afile")
    dfs.rm("/", recursive=True)
    assert not dfs.ls("/")


def test_rm_errors(dfs):
    with pytest.raises(FileNotFoundError):
        dfs.rm(["/dir", "/dir2"], recursive=True)


def test_mkdir(dfs):
    dfs.mkdir("/dir")
    dfs.touch("/afile")
    with pytest.raises(FileExistsError):
        dfs.mkdir("/dir")

    with pytest.raises(FileExistsError):
        dfs.mkdir("/afile")

    with pytest.raises(NotADirectoryError):
        dfs.mkdir("/afile/foo")

    dfs.mkdir("/dir/dir1/dir2")
    assert dfs.isdir("/dir/dir1/dir2")


def test_mkdir_no_parents(dfs):
    dfs.mkdir("/dir", create_parents=False)
    dfs.touch("/afile")
    with pytest.raises(FileExistsError):
        dfs.mkdir("/dir", create_parents=False)

    with pytest.raises(FileExistsError):
        dfs.mkdir("/afile", create_parents=False)

    with pytest.raises(NotADirectoryError):
        dfs.mkdir("/afile/foo", create_parents=False)

    with pytest.raises(FileNotFoundError):
        dfs.mkdir("/dir/dir1/dir2", create_parents=False)


def test_makedirs(dfs):
    dfs.touch("/afile")
    dfs.makedirs("/dir1/dir2")
    assert dfs.isdir("/dir1/dir2")

    with pytest.raises(FileExistsError):
        dfs.makedirs("/dir1/dir2")

    with pytest.raises(NotADirectoryError):
        dfs.makedirs("/afile/foo")

    with pytest.raises(FileExistsError):
        dfs.makedirs("/dir1/dir2/dir3")


def test_makedirs_exist_ok(dfs):
    dfs.touch("/afile")
    dfs.makedirs("/dir1/dir2", exist_ok=True)
    assert dfs.isdir("/dir1/dir2")
    dfs.makedirs("/dir1/dir2", exist_ok=True)

    with pytest.raises(NotADirectoryError):
        dfs.makedirs("/afile/foo", exist_ok=True)

    dfs.makedirs("/dir1/dir2/dir3", exist_ok=True)
    assert dfs.isdir("/dir1/dir2/dir3")


def test_rewind(dfs):
    # https://github.com/fsspec/filesystem_spec/issues/349
    dfs.mkdir("src")
    with dfs.open("src/file.txt", "w") as f:
        f.write("content")
    with dfs.open("src/file.txt") as f:
        assert f.tell() == 0


def test_no_rewind_append_mode(dfs):
    # https://github.com/fsspec/filesystem_spec/issues/349
    dfs.mkdir("src")

    with dfs.open("src/file.txt", "w") as f:
        f.write("content")
    with dfs.open("src/file.txt", "a") as f:
        assert f.tell() == 7


def test_seekable(dfs):
    fn0 = "foo.txt"
    with dfs.open(fn0, "wb") as f:
        f.write(b"data")

    f = dfs.open(fn0, "rt")
    assert f.seekable(), "file is not seekable"
    f.seek(1)
    assert f.read(1) == "a"
    assert f.tell() == 2


def test_try_open_directory(dfs):
    dfs.mkdir("/dir")
    with pytest.raises(IsADirectoryError):
        dfs.open("dir")


def test_try_open_not_existing_file(dfs):
    with pytest.raises(FileNotFoundError):
        dfs.open("not-existing-file")


def test_try_open_file_on_super_prefix(dfs):
    dfs.touch("/afile")
    with pytest.raises(NotADirectoryError):
        dfs.open("/afile/file")


def test_created(dfs):
    dfs.mkdir("/dir")
    dfs.touch("/dir/afile")
    assert dfs.created("/dir/afile") == dfs.store.get(["dir", "afile"]).created
    assert dfs.created("/dir") is None


def test_cp_file(dfs):
    dfs.pipe_file("/afile", b"content")
    dfs.cp_file("/afile", "/bfile")
    assert dfs.cat_file("/bfile") == dfs.cat_file("/afile") == b"content"


def test_cp_file_directory(dfs):
    dfs.mkdir("/dir")
    dfs.cp_file("/dir", "/dir2")
    assert dfs.isdir("/dir")


def test_transaction(dfs):
    dfs.start_transaction()
    dfs.mkdir("/dir")
    dfs.touch("/dir/afile")
    assert dfs.find("/") == []
    dfs.end_transaction()
    assert dfs.find("/") == ["/dir/afile"]

    with dfs.transaction:
        dfs.touch("/dir/bfile")
        assert dfs.find("/") == ["/dir/afile"]
    assert dfs.find("/") == ["/dir/afile", "/dir/bfile"]
