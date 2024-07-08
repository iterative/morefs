import errno
import os
import shutil
from typing import List

import fsspec


class OverlayFileSystem(fsspec.AbstractFileSystem):  # pylint: disable=abstract-method
    cachable = False

    def __init__(self, *fses: fsspec.AbstractFileSystem, **kwargs):
        storage_options = {
            key: value for key, value in kwargs.items() if key.startswith("fs_")
        }
        self.fses: List[fsspec.AbstractFileSystem] = list(fses)
        self.fses.extend(kwargs.pop("filesystems", []))
        for proto, options in kwargs.items():
            if proto.startswith("fs_"):
                continue
            if options is None:
                options = {}
            self.fses.append(fsspec.filesystem(proto, **options))
        super().__init__(*self.fses, **storage_options)

    @property
    def upper_fs(self):
        return self.fses[0]

    def __getattr__(self, proto):
        for fs in self.fses:
            if isinstance(fs.protocol, str):
                protocols = (fs.protocol,)
            else:
                protocols = fs.protocol

            for fs_proto in protocols:
                if proto == fs_proto:
                    setattr(self, proto, fs)
                    return fs
        raise AttributeError

    def ls(self, path, detail=False, **kwargs):
        listing = []
        for fs in self.fses:
            try:
                listing.extend(fs.ls(path, detail=detail, **kwargs))
            except (FileNotFoundError, NotImplementedError):
                continue

        if not detail:
            return sorted({item.strip("/") for item in listing})

        out = {}
        for item in listing:
            name = item["name"].strip("/")
            out.setdefault(name, {**item, "name": name})

        return [item for _, item in sorted(out.items())]

    @staticmethod
    def _iterate_fs_with(func):
        def inner(self, path, *args, **kwargs):
            for fs in self.fses:
                try:
                    return getattr(fs, func)(path, *args, **kwargs)
                except (
                    FileNotFoundError,
                    NotImplementedError,
                    AttributeError,
                ):
                    continue
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        return inner

    @staticmethod
    def _raise_readonly(path, *args, **kwargs):
        raise OSError(errno.EROFS, os.strerror(errno.EROFS), path)

    info = _iterate_fs_with.__get__(object)("info")
    created = _iterate_fs_with.__get__(object)("created")
    modified = _iterate_fs_with.__get__(object)("modified")

    def mkdir(self, path, create_parents=True, **kwargs):
        # if create_parents is False:
        if self.exists(path):
            raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), path)
        parent = self._parent(path)
        if not create_parents and not self.isdir(parent):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        self.upper_fs.mkdir(path, create_parents=True, **kwargs)

    def makedirs(self, path, exist_ok=False):
        self.upper_fs.makedirs(path, exist_ok=exist_ok)

    def rmdir(self, path):
        self.upper_fs.rmdir(path)

    def _rm(self, path):
        self.upper_fs._rm(path)  # pylint: disable=protected-access

    def cp_file(self, path1, path2, **kwargs):
        src_fs = None
        for fs in self.fses:
            if fs.exists(path1):
                src_fs = fs
                break

        if not src_fs:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path1)
        if src_fs == self.upper_fs:
            return src_fs.cp_file(path1, path2)

        with src_fs.open(path1) as src, self.upper_fs.open(path2, "wb") as dst:
            shutil.copyfileobj(src, dst)

    def _open(self, path, mode="rb", **kwargs):  # pylint: disable=arguments-differ
        if "rb" in mode:
            for fs in self.fses:
                try:
                    # pylint: disable=protected-access
                    return fs._open(path, mode=mode, **kwargs)
                except (
                    FileNotFoundError,
                    NotImplementedError,
                    AttributeError,
                ):
                    continue
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        if "ab" in mode:
            try:
                info = self.upper_fs.info(path)
                if info["type"] == "directory":
                    raise IsADirectoryError(
                        errno.EISDIR,
                        os.strerror(errno.EISDIR),
                        path,
                    )
            except FileNotFoundError as exc:
                for fs in self.fses[1:]:
                    try:
                        info = fs.info(path)
                        if info["type"] == "directory":
                            raise IsADirectoryError(
                                errno.EISDIR,
                                os.strerror(errno.EISDIR),
                                path,
                            ) from exc
                        return self._raise_readonly(path)
                    except (
                        FileNotFoundError,
                        NotImplementedError,
                        AttributeError,
                    ):
                        continue
        # pylint: disable=protected-access
        return self.upper_fs._open(path, mode=mode, **kwargs)

    def sign(self, path, expiration=100, **kwargs):
        return self.upper_fs.sign(path, expiration, **kwargs)

    if hasattr(fsspec.AbstractFileSystem, "fsid"):

        @property
        def fsid(self):
            return "overlay_" + "+".join(fs.fsid for fs in self.fses)
