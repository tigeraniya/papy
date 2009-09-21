# -*- coding: utf-8 -*-
# -*- Import: -*-
#
NAME = 'PaPy'
VERSION = '1.0b1'

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

PACKAGES = ['papy', 'IMap', 'papy.workers', 'papy.utils', 'papy.tkgui']
SCRIPTS = ['src/papy/tkgui/papy']

PACKAGE_DIR = {'papy': 'src/papy', 'IMap': 'src/IMap'}
PACKAGE_DATA = {'papy':['utils/templates/*', 'rpycd/*', 'icons/*']}

REQUIRES = [
    # -*- Install requires: -*-
    'setuptools',
    ]

# compatible with distutils of python 2.3+ or later
paver.setuputils.setup(
    name=NAME.lower(),
    version=VERSION,

    description='Parallel pipelines for Python',
    keywords='multiprocessing parallel pipeline workflow rpyc',
    author='Marcin Cieslik',
    author_email='mpc4p@virginia.edu',
    url='http://muralab.org/PaPy',
    license='GPLv3',
    long_description=open('README.rst', 'r').read(),

    include_package_data=True,
    classifiers=CLASSIFIERS,
    packages=PACKAGES,
    scripts=SCRIPTS,
    package_dir=PACKAGE_DIR,
    package_data=PACKAGE_DATA,
    install_requires=REQUIRES,
    zip_safe=False,
    )

#paver.easy
options(
    # -*- Paver options: -*-
    minilib=Bunch(
        extra_files=[
            'doctools', 'virtual'
            # -*- Minilib extra files: -*-
            ]
        ),
    sphinx=Bunch(
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
    for pat in ['src', 'test', 'doc']:
        for log in path.walkfiles(path(pat), 'PaPy_log*'):
            log.remove()


@task
@needs('generate_setup', 'minilib', 'setuptools.command.sdist')
def sdist():
    """Overrides sdist to make sure that our setup.py is generated."""

@needs('setuptools.command.sdist')
def snapshot():
    """"""
    tarfile = path('dist').listdir()[0]
    tarfile.move(path('snapshots') / tarfile.name)

@task
@needs('paver.doctools.html')
def html_local():
    """Build documentation and install it into papy/doc/build"""

@task
@needs('paver.doctools.html')
def html_remote():
    """Build documentation and install it into papy/doc/html"""
    # get built documentati
    builtdocroot = path("doc") / options.sphinx.builddir
    builtdochtml = builtdocroot / "html"
    # find destination
    dochtml = path("doc") / "html"
    for f in builtdochtml.walk():
        if f.basename() in ('.svn', '.buildinfo'):
            try:
                f.rmtree()
            except OSError:
                f.remove()
    try:
        sh('svn del doc/html/*')
    except Exception, e:
        print e
    sh('cd doc/html; svn commit -m "documentation update:remove"; svn update; cd ../..')
    for i in builtdochtml.glob('*'):
        i.move(dochtml)
    sh('svn add doc/html/*')
    sh('cd doc/html/; ../do_mime.sh; svn commit -m "documentation update:add"')
    builtdocroot.rmtree()









