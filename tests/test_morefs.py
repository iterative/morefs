"""Tests for `morefs` package."""

import fsspec
import pytest
from morefs.dict import DictFS
from morefs.memory import MemFS


@pytest.mark.parametrize("proto, fs_cls", [("dictfs", DictFS), ("memfs", MemFS)])
def test_fsspec(proto, fs_cls):
    fs = fsspec.filesystem(proto)
    assert isinstance(fs, fs_cls)
