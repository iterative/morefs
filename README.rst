morefs
======

|PyPI| |Status| |Python Version| |License|

|Tests| |Codecov| |pre-commit| |Black|

.. |PyPI| image:: https://img.shields.io/pypi/v/morefs.svg
   :target: https://pypi.org/project/morefs/
   :alt: PyPI
.. |Status| image:: https://img.shields.io/pypi/status/morefs.svg
   :target: https://pypi.org/project/morefs/
   :alt: Status
.. |Python Version| image:: https://img.shields.io/pypi/pyversions/morefs
   :target: https://pypi.org/project/morefs
   :alt: Python Version
.. |License| image:: https://img.shields.io/pypi/l/morefs
   :target: https://opensource.org/licenses/Apache-2.0
   :alt: License
.. |Tests| image:: https://github.com/iterative/morefs/workflows/Tests/badge.svg
   :target: https://github.com/iterative/morefs/actions?workflow=Tests
   :alt: Tests
.. |Codecov| image:: https://codecov.io/gh/iterative/morefs/branch/main/graph/badge.svg
   :target: https://app.codecov.io/gh/iterative/morefs
   :alt: Codecov
.. |pre-commit| image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit
.. |Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
   :alt: Black


Features
--------

*morefs* provides standalone fsspec-based filesystems like:

* ``AsyncLocalFileSystem`` that provides async implementation of ``LocalFileSystem``.
* In-memory filesystems ``DictFileSystem`` built on nested dictionaries and ``MemFS`` built on tries, and are much faster than fsspec's ``MemoryFileSystem``.
* ``OverlayFileSystem`` that allows to overlay multiple fsspec-based filesystems.

Installation
------------

You can install *morefs* via pip_ from PyPI_:

.. code:: console

   $ pip install morefs

You might need to install with extras for some filesystems:

.. code:: console

   $ pip install morefs[asynclocal]  # for installing aiofile dependency for AsyncLocalFileSystem
   $ pip install morefs[memfs]  # for installing pygtrie dependency for MemFS


Usage
-----

AsyncLocalFileSystem
~~~~~~~~~~~~~~~~~~~~

Extended version of ``LocalFileSystem`` that also provides async methods.

.. code:: python

    import asyncio
    from morefs.asyn_local import AsyncLocalFileSystem

    async def main():
        fs = AsyncLocalFileSystem(auto_mkdir=False)

        f = await fs.open_async("foo", mode="w")
        async with f:
            await f.write("foobar")

        content = await fs._cat("foo")
        print(content)
        print(fs.cat("foo"))  # you can still use sync methods

    asyncio.run(main())


DictFS
~~~~~~

DictFS is a nested dictionary-based, in-memory filesystem
and acts more like a real LocalFileSystem.

.. code:: python

    from morefs.dict import filesystem

    fs = DictFS()


MemFS
~~~~~

MemFS is a trie-based in-memory filesystem, and acts like a bucket storage.

.. code:: python

    from morefs.memory import MemFS

    fs = MemFS()


OverlayFileSystem
~~~~~~~~~~~~~~~~~

.. code:: python

    from morefs.overlay import OverlayFileSystem

    # use localfilesystem for write, overlay all filesystems for read
    fs = OverlayFileSystem(file={"auto_mkdir": True}, s3={"anon": True})
    # or you can pass filesystem instances directly
    # as variable positional arguments or with keyword argument `filesystems=[]`
    fs = OverlayFileSystem(LocalFileSystem(), s3={"anon": True})


Contributing
------------

Contributions are very welcome.
To learn more, see the `Contributor Guide`_.


License
-------

Distributed under the terms of the `Apache 2.0 license`_,
*morefs* is free and open source software.


Issues
------

If you encounter any problems,
please `file an issue`_ along with a detailed description.


.. _Apache 2.0 license: https://opensource.org/licenses/Apache-2.0
.. _PyPI: https://pypi.org/
.. _file an issue: https://github.com/iterative/morefs/issues
.. _pip: https://pip.pypa.io/
.. github-only
.. _Contributor Guide: CONTRIBUTING.rst
