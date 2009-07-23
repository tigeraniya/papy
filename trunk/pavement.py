# -*- coding: utf-8 -*-
# -*- Import: -*-
#
NAME = 'PaPy'
VERSION = '1.0'

# python
import os
import sys

# paver
from paver.easy import *
from paver.path25 import path
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

PACKAGES =['papy', 'IMap', 'papy.workers', 'papy.utils', 'papy.tkgui']
SCRIPTS = ['src/papy/tkgui/papy']

PACKAGE_DIR ={'papy': 'src/papy', 'IMap': 'src/IMap'}
PACKAGE_DATA ={'papy.utils':['templates/*', 'rpycd/*'], 'papy.tkgui':['icons/*']}

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
    scripts = SCRIPTS,
    package_dir = PACKAGE_DIR,
    package_data = PACKAGE_DATA,
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
        docroot="doc",
        builddir="build",
        sourcedir="source"
        ),
    )

# bdist etc.
paver.setuputils.install_distutils_tasks()


@task
def clean():
    """Cleans up the virtualenv"""
    # remove build and dist and junk
    for p in ('build', 'dist', 'doc/build', 'include', \
              'paver-minilib.zip', 'papy.egg-info', 'setup.py'):
         pth = path(p)
         if pth.isdir():
             pth.rmtree()
         elif pth.isfile():
             pth.remove()
    for pyc in path.walkfiles(path('src'), '*.pyc'):
        pyc.remove()


@task
@needs('generate_setup', 'minilib', 'setuptools.command.sdist')
def sdist():
    """Overrides sdist to make sure that our setup.py is generated."""

@task
@needs('paver.doctools.html')
def html():
    """Build documentation and install it into papy/docs"""
    print options.sphinx.docroot, options.sphinx.builddir, "html"
    builtdocs = path("doc") / options.sphinx.builddir / "html"








