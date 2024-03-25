from os import fspath
from unittest.mock import ANY

import pytest
from morefs.memory import MemFS


@pytest.fixture
def m():
    return MemFS()


def test_memfs_should_not_be_cached():
    assert MemFS() is not MemFS()


def test_1(m):
    m.touch("/somefile")  # NB: is found with or without initial /
    m.touch("afiles/and/another")
    files = m.find("")
    assert files == ["/afiles/and/another", "/somefile"]

    files = sorted(m.get_mapper())
    assert files == ["afiles/and/another", "somefile"]


def test_strip(m):
    assert m._strip_protocol("") == ""
    assert m._strip_protocol("memory://") == ""
    assert m._strip_protocol("afile") == "/afile"
    assert m._strip_protocol("/b/c") == "/b/c"
    assert m._strip_protocol("/b/c/") == "/b/c"


def test_put_single(m, tmp_path):
    fn = tmp_path / "dir"
    fn.mkdir()

    (fn / "abc").write_bytes(b"text")
    m.put(fspath(fn), "/test")  # no-op, no files
    assert not m.exists("/test/abc")
    assert not m.exists("/test/dir")
    m.put(fspath(fn), "/test", recursive=True)
    assert m.cat("/test/abc") == b"text"


def test_ls(m):
    m.touch("/dir/afile")
    m.touch("/dir/dir1/bfile")
    m.touch("/dir/dir1/cfile")

    assert m.ls("/", False) == ["/dir"]
    assert m.ls("/dir", False) == ["/dir/afile", "/dir/dir1"]
    assert m.ls("/dir", True)[0]["type"] == "file"
    assert m.ls("/dir", True)[1]["type"] == "directory"

    assert len(m.ls("/dir/dir1")) == 2
    assert m.ls("/dir/afile") == ["/dir/afile"]
    assert m.ls("/dir/dir1/bfile") == ["/dir/dir1/bfile"]
    assert m.ls("/dir/dir1/cfile") == ["/dir/dir1/cfile"]

    with pytest.raises(FileNotFoundError):
        m.ls("/dir/not-existing-file")


def test_mv_recursive(m):
    m.mkdir("src")
    m.touch("src/file.txt")
    m.mv("src", "dest", recursive=True)
    assert m.exists("dest/file.txt")
    assert not m.exists("src")


def test_rm(m):
    m.touch("/dir1/dir2/file")
    m.rm("/dir1", recursive=True)
    assert not m.exists("/dir1/dir2/file")
    assert not m.exists("/dir1/dir2")
    assert not m.exists("/dir1")

    with pytest.raises(FileNotFoundError):
        m.rm("/dir1", recursive=True)


def test_rm_multiple_files(m):
    m.touch("/dir/file1")
    m.touch("/dir/file2")

    m.rm(["/dir/file1", "/dir/file2"])
    assert not m.ls("/")


def test_rm_file(m):
    m.touch("/dir/file")
    with pytest.raises(IsADirectoryError):
        m.rm_file("/dir")

    with pytest.raises(FileNotFoundError):
        m.rm_file("/dir/foo")

    m.rm_file("/dir/file")
    assert not m.exists("/dir/file")


def test_rewind(m):
    # https://github.com/fsspec/filesystem_spec/issues/349
    with m.open("src/file.txt", "w") as f:
        f.write("content")
    with m.open("src/file.txt") as f:
        assert f.tell() == 0


def test_no_rewind_append_mode(m):
    # https://github.com/fsspec/filesystem_spec/issues/349
    with m.open("src/file.txt", "w") as f:
        f.write("content")
    with m.open("src/file.txt", "a") as f:
        assert f.tell() == 7


def test_seekable(m):
    fn0 = "foo.txt"
    with m.open(fn0, "wb") as f:
        f.write(b"data")

    f = m.open(fn0, "rt")
    assert f.seekable(), "file is not seekable"
    f.seek(1)
    assert f.read(1) == "a"
    assert f.tell() == 2


def test_try_open_directory(m):
    m.touch("/dir/file")
    with pytest.raises(IsADirectoryError):
        m.open("dir")


def test_try_open_not_existing_file(m):
    with pytest.raises(FileNotFoundError):
        m.open("not-existing-file")


def test_try_open_file_on_super_prefix(m):
    m.touch("/afile")
    with pytest.raises(NotADirectoryError):
        m.open("/afile/file")


def test_empty_raises(m):
    with pytest.raises(FileNotFoundError):
        m.ls("nonexistent")

    with pytest.raises(FileNotFoundError):
        m.info("nonexistent")


def test_moves(m):
    m.touch("source.txt")
    m.mv("source.txt", "target.txt")

    m.touch("source2.txt")
    m.mv("source2.txt", "target2.txt", recursive=True)
    assert m.find("") == ["/target.txt", "/target2.txt"]


def test_remove_all(m):
    m.touch("afile")
    m.rm("/", recursive=True)
    assert not m.ls("/")


def test_created(m):
    m.touch("/dir/afile")
    assert m.created("/dir/afile") == m.trie["/dir/afile"].created
    assert m.created("/dir") is None


def test_info(m):
    m.touch("/dir/file")

    assert m.info("/") == {"name": "", "size": 0, "type": "directory"}
    assert m.info("/dir") == {"name": "/dir", "size": 0, "type": "directory"}
    assert m.info("/dir/file") == {
        "name": "/dir/file",
        "size": 0,
        "type": "file",
        "created": ANY,
    }

    with pytest.raises(FileNotFoundError):
        m.info("/not-existing-file")


def test_cp_file(m):
    m.pipe_file("/afile", b"content")
    m.cp_file("/afile", "/bfile")
    assert m.cat_file("/bfile") == m.cat_file("/afile") == b"content"


def test_transaction(m):
    m.start_transaction()
    m.touch("/dir/afile")
    assert m.find("/") == []
    m.end_transaction()
    assert m.find("/") == ["/dir/afile"]

    with m.transaction:
        m.touch("/dir/bfile")
        assert m.find("/") == ["/dir/afile"]
    assert m.find("/") == ["/dir/afile", "/dir/bfile"]
