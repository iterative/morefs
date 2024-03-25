import errno
import os
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFile

ContainerOrFile = Union[Dict[str, Dict], "DictFile"]


class Store(dict):
    def __init__(self, paths: Iterable[str] = ()) -> None:
        super().__init__()
        self.paths = tuple(paths)

    def new_child(self, paths: Iterable[str]) -> None:
        self.set(paths, type(self)(paths=paths))

    def set(self, paths: Iterable[str], value: Any, overwrite: bool = False) -> None:
        if not paths:
            raise ValueError("no path supplied")

        *rest, key = paths
        child = self.get(rest)

        if not overwrite and key in child:
            raise ValueError("cannot overwrite - item exists")
        child[key] = value

    def get(self, paths: Iterable[str]) -> "ContainerOrFile":  # type: ignore[override]
        child = self
        for path in paths:
            child = child[path]
        return child

    def delete(self, paths: Iterable[str]) -> None:
        if not paths:
            self.clear()
            return

        *rest, key = paths
        child = self.get(rest)
        del child[key]


def oserror(code: int, path: str) -> OSError:
    return OSError(code, os.strerror(code), path)


class DictFS(AbstractFileSystem):  # pylint: disable=abstract-method
    cachable = False
    protocol = "dictfs"
    root_marker = ""

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        if path.startswith("dictfs://"):
            path = path[len("dictfs://") :]
        if "::" in path or "://" in path:
            return path.rstrip("/")
        path = path.lstrip("/").rstrip("/")
        return "/" + path if path else cls.root_marker

    def __init__(self, store: Optional[Store] = None) -> None:
        super().__init__()
        if store is None:
            store = Store()
        self.store = store

    def _info(
        self,
        path: str,
        item: ContainerOrFile,
        file: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if isinstance(item, dict):
            return {"name": path, "size": 0, "type": "directory"}
        assert isinstance(item, DictFile)
        return item.to_json(file=file)

    @classmethod
    @lru_cache(maxsize=1000)
    def path_parts(cls, path: str) -> Tuple[str, ...]:
        path = cls._strip_protocol(path)
        if path == "/":
            return ()
        _root_marker, *parts = path.split(cls.sep)
        return tuple(parts)

    @classmethod
    @lru_cache(maxsize=1000)
    def join_paths(cls, paths: Tuple[str, ...]) -> str:
        if not paths:
            return cls.root_marker
        return cls.sep.join([cls.root_marker, *paths])

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        paths = self.path_parts(path)
        normpath = self.join_paths(paths)
        try:
            item = self.store.get(paths)
        except KeyError as exc:
            raise oserror(errno.ENOENT, normpath) from exc
        except TypeError as exc:
            raise oserror(errno.ENOTDIR, normpath) from exc
        return self._info(normpath, item, **kwargs)

    def ls(self, path: str, detail: bool = False, **kwargs: Any):
        paths = self.path_parts(path)
        normpath = self.join_paths(paths)

        try:
            item = self.store.get(paths)
        except KeyError as exc:
            raise oserror(errno.ENOENT, normpath) from exc
        except TypeError as exc:
            raise oserror(errno.ENOTDIR, normpath) from exc

        if not isinstance(item, dict):
            if not detail:
                return [normpath]
            return [self._info(normpath, item)]

        entries: Iterable[Tuple[str, ContainerOrFile]] = item.items()
        if kwargs.get("sort"):
            entries = sorted(entries)

        if not detail:
            return [self.join_paths((*paths, key)) for key, _ in entries]
        return [
            self._info(self.join_paths((*paths, key)), value) for key, value in entries
        ]

    def _rm(self, path: str) -> None:
        info = self.info(path)
        paths = self.path_parts(path)
        normpath = self.join_paths(paths)
        if info["type"] == "directory":
            raise oserror(errno.EISDIR, normpath)
        return self._rm_paths(paths)

    def _rm_paths(self, paths: Tuple[str, ...]) -> None:
        normpath = self.join_paths(paths)
        try:
            self.store.delete(paths)
        except TypeError as exc:
            raise oserror(errno.ENOTDIR, normpath) from exc
        except KeyError as exc:
            raise oserror(errno.ENOENT, normpath) from exc

    def rmdir(self, path: str) -> None:
        info = self.info(path)
        paths = self.path_parts(path)
        normpath = self.join_paths(paths)

        if info["type"] == "file":
            raise oserror(errno.ENOTDIR, normpath)

        if self.ls(path):
            raise oserror(errno.ENOTEMPTY, normpath)
        self._rm_paths(paths)

    def mkdir(self, path: str, create_parents: bool = True, **kwargs) -> None:
        paths = self.path_parts(path)
        normpath = self.join_paths(paths)
        try:
            _ = self.store.get(paths)
            raise oserror(errno.EEXIST, normpath)
        except KeyError:
            pass
        except TypeError as exc:
            raise oserror(errno.ENOTDIR, normpath) from exc

        if create_parents:
            return self.makedirs(path, exist_ok=True)
        self._mkdir_paths(paths)

    def _mkdir_paths(self, paths: Tuple[str, ...]) -> None:
        normpath = self.join_paths(paths)
        try:
            self.store.new_child(paths)
        except TypeError as exc:
            raise oserror(errno.ENOTDIR, normpath) from exc
        except ValueError as exc:
            raise oserror(errno.EEXIST, normpath) from exc
        except KeyError as exc:
            raise oserror(errno.ENOENT, normpath) from exc

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        paths = self.path_parts(path)
        normpath = self.join_paths(paths)
        try:
            _ = self.store.get(paths)
            if not exist_ok:
                raise oserror(errno.EEXIST, normpath)
            return
        except KeyError:
            pass
        except TypeError as exc:
            raise oserror(errno.ENOTDIR, normpath) from exc

        for idx in range(len(paths)):
            try:
                self._mkdir_paths(paths[: idx + 1])
            except FileExistsError:
                if not exist_ok:
                    raise

    def _open(  # noqa: PLR0913
        self,
        path: str,
        mode: str = "rb",
        block_size=None,  # noqa: ARG002
        autocommit=True,  # noqa: ARG002
        cache_options=None,  # noqa: ARG002
        **kwargs,
    ) -> "DictFile":
        paths = self.path_parts(path)
        normpath = self.join_paths(paths)

        try:
            info = self.info(path, file=True)
            if info["type"] == "directory":
                raise oserror(errno.EISDIR, normpath)
        except FileNotFoundError:
            if mode in ["rb", "ab", "rb+"]:
                raise

        if mode == "wb":
            file = DictFile(self, normpath, data=kwargs.get("data"))
            if not self._intrans:
                file.commit()
        else:
            file = info["file"]
            file.seek(0, os.SEEK_END if mode == "ab" else os.SEEK_SET)
        return file

    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        try:
            src = self.open(path1, "rb")
        except IsADirectoryError:
            self.mkdir(path2)
            return

        file = DictFile(self, path2, src.getvalue())  # implicit copy
        file.commit()

    def created(self, path: str) -> Optional[datetime]:
        return self.info(path).get("created")

    def rm(
        self,
        path: Union[str, List[str]],
        recursive: bool = False,
        maxdepth: Optional[int] = None,
    ) -> None:
        if isinstance(path, str):
            paths = [path]
        else:
            paths = path

        if recursive and not maxdepth:
            for p in paths:
                self._rm_paths(self.path_parts(p))
            return

        paths = self.expand_path(paths, recursive=recursive, maxdepth=maxdepth)
        for p in reversed(paths):
            if p in ("", "/"):
                continue
            if self.isfile(p):
                self.rm_file(p)
            else:
                self.rmdir(p)

    def pipe_file(self, path: str, value, **kwargs) -> None:
        self.open(path, "wb", data=value)


class DictFile(MemoryFile):
    def commit(self) -> None:
        fs = self.fs
        paths = fs.path_parts(self.path)
        try:
            fs.store.set(paths, self, overwrite=True)
        except TypeError as exc:
            raise oserror(errno.ENOTDIR, self.path) from exc
        except ValueError as exc:
            raise oserror(errno.EEXIST, self.path) from exc
        except KeyError as exc:
            raise oserror(errno.ENOENT, self.path) from exc

    def to_json(self, file: bool = False) -> Dict[str, Any]:
        details = {
            "name": self.path,
            "size": self.size,
            "type": "file",
            "created": self.created,
        }
        if file:
            details["file"] = self
        return details
