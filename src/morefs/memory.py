import errno
import os

import pygtrie
from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFile
from fsspec.implementations.memory import MemoryFileSystem as _MemFS


class MemFS(AbstractFileSystem):  # pylint: disable=abstract-method
    """In-Memory Object Storage FileSystem based on Trie data-structure."""

    cachable = False
    protocol = ("memfs", "memory")

    def __init__(self, *args, **storage_options):
        super().__init__(*args, **storage_options)
        self.trie = self.store = pygtrie.StringTrie()

    @classmethod
    def _strip_protocol(cls, path):
        if path.startswith("memfs://"):
            path = path[len("memfs://") :]
        return _MemFS._strip_protocol(path)  # pylint: disable=protected-access

    @staticmethod
    def _info(path, filelike=None, **kwargs):
        if filelike:
            return {
                "name": path,
                "size": (
                    filelike.size
                    if hasattr(filelike, "size")
                    else filelike.getbuffer().nbytes
                ),
                "type": "file",
                "created": getattr(filelike, "created", None),
            }
        return {"name": path, "size": 0, "type": "directory"}

    def ls(self, path, detail=False, **kwargs):
        path = self._strip_protocol(path)
        out = []

        def node_factory(path_conv, parts, children, filelike=None):
            node_path = path_conv(parts)
            if path == node_path and children:
                list(children)
                return

            info = self._info(node_path, filelike) if detail else node_path
            out.append(info)

        try:
            self.trie.traverse(node_factory, prefix=path)
        except KeyError as exc:
            if path in ("", "/"):
                return []
            raise FileNotFoundError(errno.ENOENT, "No such file", path) from exc

        return out

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        if path in ("", "/") or self.trie.has_subtrie(path):
            return self._info(path, **kwargs)
        if filelike := self.trie.get(path):
            return self._info(path, filelike, **kwargs)

        short = self.trie.shortest_prefix(path)
        if short and short.key != path:
            raise NotADirectoryError(errno.ENOTDIR, "Not a directory", path)
        raise FileNotFoundError(errno.ENOENT, "No such file", path)

    def _rm(self, path):
        path = self._strip_protocol(path)
        if self.isdir(path):
            raise IsADirectoryError(errno.EISDIR, "Is a directory", path)

        try:
            del self.trie[path]
        except KeyError as e:
            raise FileNotFoundError(errno.ENOENT, "No such file", path) from e

    def rm(self, path, recursive=False, maxdepth=None):
        paths = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        for p in paths:
            self.store.pop(p, None)

    def _open(self, path, mode="rb", **kwargs):  # pylint: disable=arguments-differ
        path = self._strip_protocol(path)
        try:
            info = self.info(path)
            if info["type"] == "directory":
                raise IsADirectoryError(errno.EISDIR, "Is a directory", path)
        except FileNotFoundError:
            if mode in ["rb", "ab", "rb+"]:
                raise

        if mode == "wb":
            filelike = MemoryFile(self, path)
            if not self._intrans:
                filelike.commit()
            return filelike

        filelike = self.trie[path]
        filelike.seek(0, os.SEEK_END if mode == "ab" else os.SEEK_SET)
        return filelike

    def cp_file(self, path1, path2, **kwargs):
        path1 = self._strip_protocol(path1)
        path2 = self._strip_protocol(path2)

        try:
            src = self.open(path1, "rb")
        except IsADirectoryError:
            return

        with src, self.open(path2, "wb") as dst:
            dst.write(src.getbuffer())

    def created(self, path):
        return self.info(path).get("created")
