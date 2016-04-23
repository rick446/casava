# Copyright 2013-2014 Synappio LLC. All rights reserved.
import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

setup(
    name='casava',
    version='0.1',
    description='casava: a csv reader thingy',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
    ],
    author='Rick Copeland',
    author_email='rick@arborian.com',
    maintainer='Rick Copeland',
    maintainer_email='rick@arborian.com',
    url='https://github.com/synappio/casava',
    keywords='csv',
    packages=find_packages(),
    include_package_data=True,
    package_data={'': ['README.txt', 'CHANGES.txt']},
    zip_safe=False,
    install_requires=[
        'chardet>=2.2.1',
    ],
    tests_require=[],
    test_suite="casava",
    entry_points="""\
    """,
)
