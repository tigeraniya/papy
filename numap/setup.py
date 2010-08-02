#!/usr/bin/env python
# -*- coding: utf-8 -*-
NAME = 'numap'
VERSION = '1.0'

import setuptools
from distutils.core import setup

REQUIRES = [
    # -*- Install requires: -*-
    'setuptools',
    'rpyc'
    ]

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Operating System :: OS Independent",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Topic :: Scientific/Engineering",
    "Programming Language :: Python :: 2.6",
    "License :: OSI Approved :: BSD License"
    ]

PACKAGES = ['numap']
PACKAGE_DIR = {'numap': 'src/numap'}
PACKAGE_DATA = {'numap':['doc/source/*', 'doc/examples/*', 'test/*']}

setup(
    name=NAME.lower(),
    version=VERSION,
    description='lazy, parallel, remote, multi-task map function',
    keywords='multiprocessing parallel map imap pool',
    author='Marcin Cieslik',
    author_email='mpc4p@virginia.edu',
    url='http://muralab.org/numap/',
    license='BSD License',
    long_description=open('README.rst', 'r').read(),
    classifiers=CLASSIFIERS,
    packages=PACKAGES,
    package_dir=PACKAGE_DIR,
    #package_data=PACKAGE_DATA,
    install_requires=REQUIRES,
    # Options
    include_package_data=True,
    zip_safe=False,
    )

