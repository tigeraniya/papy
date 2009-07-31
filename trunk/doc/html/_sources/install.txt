Installation
============

This guide will go through the steps required to install a bleeding-edge
version of *PaPy* on a UNIX machine using the ``bash`` shell.

       
Installing PaPy the easy way
++++++++++++++++++++++++++++ 

*PaPy* is indexed on the *PyPI* (Python Package Index). And can be installed 
simply via::

    $ su -c "easy_install papy"
    
If this did not work please try to use the source snapshot.

    * download a snapshot from the project page
    
    * install it via easy_install::
    
        su -c "easy_install papy-1.0.tar.gz"

The optional run-time dependencies of *PaPy* are:

    * *RPyC*            (``rpyc``)
    * *Pmw*             (``Pmw``)
    * ``posix_ipc``
    * *MySQL-python*    (``MySQLdb``)
    
Optional dependencies to install/build/deploy *PaPy* are:

    * ``easy_install``  (``setuptools``)
    * *Sphinx*          (``sphinx``)
    * *Paver*           (``paver``)
    
*PaPy* and Python development is much easier using the following tools:

    * ``virtualenv``
    * ``virtualevnwrapper``

An easy install assumes that you default interpreter is Python2.6 (in future 
Python2.5 will also be supported), you have ``setuptools`` installed and you 
want to install *PaPy* into system-wide site-packages (the location where Python
looks for installed libraries). If the default Python interpreter for your 
operating system is different from Python2.6 or you do not want to put *PaPy* or 
its optional dependencies into the system directories or finally you'd like the 
latest source-code revision of *PaPy* read further.


Installing PaPy the fancy way
+++++++++++++++++++++++++++++

The fancy (and cleaner) way of using PaPy is to create a virtual environment and
use PaPy sources checked out from the ``subversion`` repository. The general 
stream  of action is to install a system-wide Python2.6 (if required) install 
``setuptools`` for Python2.6 (if required). Providing virtual environment 
support by installing ``virtualenv`` and ``virtualenvwrapper`` for the Python2.6
interpreter. Installing PaPy optional dependencies and build-tools and finally 
checking out PaPy from the source-code repository and providing the library in 
the virtual environment. This guide assumes you are using ``bash`` and a recent 
version of something UNIX-like. 


Getting Python
______________

Most distributions will ship a recent version of Python. You can and should skip
this step if you have Python 2.6 and it is the default Python interpreter. 
To check this open a shell and type::

    $ python
    
This should return something similar to this::

    Python 2.6.2 (r262:71600, Jun 12 2009, 10:38:05)
    [GCC 4.1.2 (Gentoo 4.1.2 p1.1)] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    
If this is correct you can go to the next step. Otherwise if the Python version 
is not 2.6.x it is possible that your operating system still provides Python2.6 
but it is not the default interpreter try::
 
    $ python2.6
    
If, and only if, this fails with ``bash: python2.6: command not found`` or 
something similar proceed with the manual system-wide installation of Python2.6 
below. This installation will not change the default Python interpreter for your
distribution. A compiled version of Python 2.6 for your distribution might be
available from third-party repositories. 
    
    #. Download Python2.6 from http://www.python.org/download/ you want a 
       source tarball and the highest 2.6.x version available. You do not want
       to install Python 3.0.x or 3.1.x
     
    #. Unpack the compressed tarball intp any directory e.g.::
  
        tar xfv Python-2.6.2.tgz
      
    #. go to the root of the unpacked directory and compile Python2.6:

        cd 
        ./configure --prefix=/usr
        make
        su -c "make altinstall"
        
       this will install an executable ``python2.6`` into ``/usr/bin``
       
    #. try if it worked::
    
        $ python2.6
        
Be careful! Some of the following steps are different depending on whether you 
build your own Python2.6 distribution or your system is using Python2.6 by 
default. Make shure yout type ``python2.6`` not ``python`` to be on the safe 
side.


Getting ``setuptools``
______________________

We will use setuptools to install PaPy, tools to create a virtual environment 
and PaPy's dependencies. This is a very common package and is most likely 
already installed. But we need it for Python2.6 To test it try::

    $ easy_install2.6

If you encounter an error, and you did *not* build Python2.6 manually you should
try to install it from your distribution's repository. On Gentoo the package
is called "dev-python/setuptools" on Fedora it is "python-setuptools" and
"python-setuptools-devel" for Ubuntu the package is called "python-setuptools"
You can install them by e.g.::

    # Gentoo
    $ su -c "emerge setuptools"
    # Ubuntu
    $ sudo apt-get install python-setuptools
    # Fedora
    $ su -c "yum install python-setuptools"
    
If you had to manually compile Python2.6 the standard distribution setuptools 
package  are most likely installed but only for the default system-wide Python 
e.g.  Python2.5. We will have to install setuptools manually for the just build 
Python2.6 interpreter.

    #. Download setuptools from http://pypi.python.org/pypi/setuptools you will 
       want the latest source version at the time of writing it is 
       setuptools-0.6c9.tar.gz.
       
    #. Unpack the compressed tarball into any directory::
    
        $ tar xvf setuptools-0.6c9.tar.gz
        
    #. Go to the root of the extracted directory::
    
        $ cd setuptools-0.6c9
    
    #. Now we install setuptools using the python2.6 executable, but first we 
       have to make shure that we don't override ``/usr/bin/easy_install``. If 
       setuptools is by default installed for a different Python interpreter.
       If there is no other Python interpreter or you do not care you can skip
       the following and just issue:
       
        # python2.6 setup.py install
       
       To prevent overriding ``/usr/bin/easy_install`` we edit the ``setupy.py``
       file::
       
        <snip>
        "console_scripts": [
            "easy_install = setuptools.command.easy_install:main",
            "easy_install-%s = setuptools.command.easy_install:main"
            % sys.version[:3]],
        <snip>
                          
       by commenting out the second line i.e.::
       
        <snip>
        "console_scripts": [
        #   "easy_install = setuptools.command.easy_install:main",
            "easy_install-%s = setuptools.command.easy_install:main"
            % sys.version[:3]],      
        <snip>
    
    #. now we can safely run the installation::
    
        $ python2.6 setup.py install
        
    #. and verify that we have ``easy_install-2.6``::
        
        # CORRECT
        $ easy_install-2.6
        error: No urls, filenames, or requirements specified (see --help)
        # NOT CORRECT
        -bash: easy_install-2.6: command not found
        
        
Creating a virtual environment
______________________________

Generally we do not want to pollute the system-wide distribution with PaPy 
and its dependencies, but we can and this step is optional, although maintanence
of PaPy might be easier in a virtual environment. We will create a virtual 
environment just for PaPy. We will install virtualenv and virtualenvwrapper into
the newly created Python installation or standard Python2.6 using 
easy_install-2.6.::

    $ su -c "easy_install-2.6 virtualenv"
    $ su -c "easy_install-2.6 virtualenvwrapper"
    
Note that these packages are installed system-wide. Now we have to configure 
virtualenvwrapper on a per-user basis. We have to edit  the ``.bashrc`` file.

    #. determine where the wrapper got installed::
    
        $ which virtualenvwrapper_bashrc
   
    #. create a directory where you will hold the virtual enviroment(s)::
    
        $ mkdir $HOME/.virtualenvs
        
    #. add the following two lines to ``~/.bashrc`` replace __REPLACE_ME__ with 
       whatever the output from the first command was.::
       
        export WORKON_HOME=$HOME/.virtualenvs
        source __REPLACE_ME__
    
Now we have to source the edited ``.bashrc`` file::

    $ source ~/.bashrc
    
This should not generate any errors. We are finally ready to create a virtual 
Python2.6 environment for PaPy.::

    $ mkvirtualenv -p python2.6 --no-site-packages papy26
    
This will install a clean virtual environment called papy26 and activate it. 
Working with virtual environments is easy. To use it type ``workon papy26`` 
to leave it type ``deactivate``.


Installing PaPy dependencies and tools
______________________________________

All PaPy dependencies are optional in the sense that the core-functionality does 
not depend on them. However using the gui, databases, posix-style shared memory
and grid functionality will require a few packages to be installed.

    * (optional) switch to the virtual environement::
    
        $ workon papy26

If you are not using a virtual environment and you did not build Python2.6 
manually you can try to install those packages from the operating system 
repository if availble. If some of them are not availble for your operating 
system or the default Python interpreter is different from Python2.6 you will 
have to install them system-wide as root: 
    
    $ su -c "easy_install-2.6 PACKAGE_NAME"
    
or::
    
    $ sudo easy_install-2.6 PACKAGE_NAME

You do not have to be root to install the packages into the virtual 
environement:

    #. install Paver to build/deploy PaPy::
    
        $ easy_install-2.6 paver
        
    #. install Sphinx to build PaPy documentation::
    
        $ easy_install-2.6 sphinx

    #. installing RPyC to use PaPy on a grid::
    
        $ easy_install-2.6 rpyc
        
       if the above did not work because the tarfile could not be downloaded 
       we have to do it manually from: 
       http://sourceforge.net/projects/rpyc/files/
       be sure to download the source distribution e.g.: rpyc-3.0.6.tar.gz
       and from the directory to which the file has been downloaded::
       
        $ easy_install rpyc-3.0.6.tar.gz

    #. installing posix_ipc for shared memory::
        
        $ easy_install-2.6 posix_ipc

    #. installing Pmw (Python Mega Widgets) for the gui. Pmw is not availble 
       from PyPI.
    
        #. Go to: http://sourceforge.net/projects/pmw/files/ and download the
           latest source tarball e.g. Pmw.1.3.2.tar.gz
           
        #. Unpack the tarball and go to the src directory::
        
            $ tar xfv Pmw.1.3.2.tar.gz
            $ cd  Pmw.1.3.2/src
            
        #. install Pmw::
        
            $ python2.6 setup.py install
        
    #. Installing mysql-python to use a MySQL database from PaPy. The package
       mysql-python is availble from PyPI, but currently the package does not
       install cleanly.  You can try this first::
       
        $ easy_install-2.6 mysql-python
        
       If you build Python2.6 or if your distribution does not provide 
       mysql-python you have to build it yourself. To do this you will need gcc,
       MySQL and MySQL header files. The respective packages are called mysql 
       and mysql-devel on Fedora, mysql and libmysql++-dev on Ubuntu and 
       just mysql on Gentoo. Make shure that you can find mysql_config and 
       mysql.h e.g.::
       
        $ which mysql_config
        /usr/bin/mysql_config
        $ ls /usr/include/mysql/mysql.h
        /usr/include/mysql/mysql.h 
    
       Now download and install MySQL-python.
    
           #. Go to: http://sourceforge.net/projects/mysql-python/files/
               and download: MySQL-python-1.2.3c1.tar.gz or a newer source 
               distribution.
               
           #. Unpack it::
            
                $ tar xvf MySQL-python-1.2.3c1.tar.gz
                
           #. Go to the unpacked directory::
            
                $ cd MySQL-python-1.2.3c1
                
           #. determine the location of mysql_config::
            
                $ which mysql_config
                
           #. make sure the ``site.cfg`` file has the correct location for the 
              mysql_config binary::
            
               # change if neccessary
               mysql_config = __REPLACE_ME__
               
           #. build and install install::
            
               $ python2.6 setup.py install
               
              If it failed make sure gcc can find the mysql.h file.
                
           #. verify it worked::
            
               $ python2.6
               >>> import MySQLdb
                
              This should not generate any errors. 
                
                
Get PaPy sources
________________

In this step we will use the latest revision of PaPy source code to either 
and provide it within the virtual environment or per-user python path.

    #. make sure you have subversion::
    
        $ svn
        Type 'svn help' for usage. 
        
       If this returns an error you have to install the ``subversion`` package::
       
        # on Gentoo
        $ su -c "emerge subversion"    
        # on Fedora
        $ su -c "yum install subversion"
        # On Ubuntu
        $ sudo apt-get install subversion
        
    #. check-out the sources::
    
        $ svn checkout http://papy.googlecode.com/svn/trunk/ papy
        
    #. We have to know where the PaPy source got copied to update add them to 
       the virtual environment or Python path.::
        
        $ cd papy/src
        
       If you decided to use a virtual environment::
        
        $ add2virtualenv .
        
       If not we update the ``$PYTHONPATH`` variable with this path in 
       ``.bashrc``::
       
        $ pwd
        SOME_PATH
        
       And add this line to ``.bashrc``. Remember to replace SOME_PATH with the 
       output from ``pwd``::
       
        export PYTHONPATH=SOME_PATH:$PYTHONPATH 
        
    #. Verify it worked.::
    
        $ python2.6
        >>> import papy
        >>> import IMap
        
        
        
      
      
      
