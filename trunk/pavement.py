# -*- Import: -*-
#
NAME = 'PaPy'
VERSION = '1.0'


# python
import os
import sys

# paver
from paver.easy import *
from paver.path import path
import paver.setuputils # find_package_data
import paver.misctasks  # generate_setup, minilib
import paver.doctools   # html output
import paver.virtual    # virtual env

CLASSIFIERS = [
    # Get more strings from http://www.python.org/pypi?%3Aaction=list_classifiers
    "Development Status :: 4 - Beta",
    "License :: OSI Approved :: GNU General Public License (GPL)",
    "Programming Language :: Python",
    "Operating System :: OS Independent"
    ]

PACKAGE_DATA = options.setup.package_data=paver.setuputils.find_package_data(
    NAME.lower(), package=NAME.lower(), only_in_packages=False)
PACKAGES = sorted(PACKAGE_DATA.keys())

REQUIRES = [
    # -*- Install requires: -*-
    'setuptools',
    ]

# compatible with distutils of python 2.3+ or later
paver.setuputils.setup(
    name =NAME.lower(),
    version =VERSION,

    description ='Parallel pipelines for Python',
    keywords='multiprocessing parallel pipeline workflow rpyc',
    author ='Marcin Cieslik',
    author_email ='mpc4p@virginia.edu',
    url ='http://muralab.org/PaPy',
    license ='GPLv3',
    long_description =open('README.rst', 'r').read(),

    include_package_data=True,
    classifiers =CLASSIFIERS,
    packages = PACKAGES,
    install_requires =REQUIRES,
    zip_safe =False,
    )

#paver.easy
options(
    # -*- Paver options: -*-
    minilib =Bunch(
        extra_files=[
            # -*- Minilib extra files: -*-
            ]
        ),
    sphinx =Bunch(
        docroot='docs',
        builddir=".build",
        sourcedir=""
        ),
    )

@task
@needs('generate_setup', 'minilib', 'setuptools.command.sdist')
def sdist():
    """Overrides sdist to make sure that our setup.py is generated."""

@task
@needs('paver.doctools.html')
def html():
    """Build documentation and install it into papy/docs"""
    builtdocs = path("docs") / options.sphinx.builddir / "html"








