DataDex
=======
A system for organizing scientific datasets.

Basic Use
=========

We have three data sets
::

    example
        dataset1
            params.txt
        dataset2
            params.txt
        dataset3
            params.txt

each of which contains a ``params.txt`` file. Within that file is a line-by-line
listing of the parameters associated with that dataset. For example:

::
    example/dataset1/params.txt:
        theta: 5
        phi: 1.3

    example/dataset2/params.txt:
        theta: 5
        phi: 1.3
    example/dataset2/params.txt:
        theta: 3

We can use ``DataDex`` to index these datasets by their parameters.

::

    >>> from datadex import DataDex
    >>> dex = DataDex()
    >>> dex.create_library("theta", "phi")
    >>> dex.index("example")
    True
    >>> dex.lookup()
    [(5, 1.3, 'example\\\\dataset1'), (5, 1.6, 'example\\\\dataset2'), (3, None, 'example\\\\dataset3')]

Once the datasets are indexed, we can search for parameter combinations:

::

    >>> dex = DataDex()
    >>> dex.search("theta = 5")
    ['example\\dataset1', 'example\\dataset2']
    >>> dex.search("phi is not null")
    ['example\\dataset1', 'example\\dataset2']
    >>> dex.search("phi is null")
    ['example\\dataset3']
    >>> dex.search("phi < 1.5")
    ['example\\dataset1']
    >>> dex.search("phi between 1 and 2")
    ['example\\dataset1', 'example\\dataset2']
    >>> dex.search("phi between 1 and 2", "theta = 3")
    []

We can also have ``DataDex`` hash the directory contents of each dataset, and
rename the dataset to that hash:

::

    >>> dex = DataDex(hash_dir=True)
    >>> dex.reset_library()
    >>> dex.lookup()
    []
    >>> dex.index("example")
    True
    >>> dex.lookup()
    [(5, 1.3, 'example\\\\91ab74d6f6c87b2c2656af1b83b009fd0c207ef415dc82ec4b328c14b5587c76'),
     (5, 1.6, 'example\\\\8d8e7755376c8c9b4406a8ba763e28814d23f55fae1ccff85c2a07c0d441f98e'),
     (3, None, 'example\\\\6be48059c8b776fab1c49d29f56e510a6838a0d857af36e6fbdf89b9461da289')]
