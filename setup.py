# Copyright 2017 Douglas G. Moore, Harrison Smith. All rights reserved.
# Use of this source code is governed by a MIT
# license that can be found in the LICENSE file.
from setuptools import setup

with open('README.rst') as f:
    README = f.read()

with open('LICENSE') as f:
    LICENSE = f.read()

DATADEX_FILES = [
    "README.rst",
    "LICENSE",
]

setup(
    name='datadex',
    version='0.0.1',
    description='A wrapper for the datadex library',
    long_description=README,
    maintainer='Douglas G. Moore',
    maintainer_email='douglas.g.moore@asu.edu',
    url='https://github.com/dglmoore/datadex',
    license=LICENSE,
    packages=['datadex'],
    package_data={'datadex' : DATADEX_FILES},
    test_suite="test",
    platforms=["Windows", "OS X", "Linux"],
)
