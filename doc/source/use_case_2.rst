Use case 2: Distributed computing.
==================================

This use case illustrates the simplest possible pipeline which utilizes remote
Python processes. Each remote computer has to expose it's computational 
resources in the form of a Python process server. A client (for example a PaPy 
IMap instace) attaches to this server and starts executing code remotely. Every 
server is a security risk and RPyC servers should only be exposed on a secure 
network (e.g. by using a firewall to restrict access from other than local
computers). We will use the RPyC 'classic_server.py' script which comes with 
RPyC, but is not installed as a binary or added to the executable path, 
therefore we will have to find it and start manually. (another option is to use
the "rpyc_server.py" daemon which comes together with PaPy, but this requires
an additional dependancy). This example will not work if the default port i.e. 
18811 is firewalled or already used.  

Finding the "classic_server.py" file on a remote unix machine 
(example result shown)::

    $ locate classic_server.py
    /usr/lib/python2.6/site-packages/rpyc-3.0.5-py2.6.egg/rpyc/servers/classic_server.py
    
(To start the RPyC server on a Winodws machine please consult the RPyC 
documentation.)
    
Starting the forking server listening on the default remote port with default 
settings (-m stands for mode)::

    $ python SOME_PATH/classic_server.py -m forking
    
From this moment the remote Python process is available as HOST:18811 and can be 
used by all Python scripts using the rpyc module not only a PaPy pipeline. For 
testing purposes the RPyC server can be started on the local computer. The 
pipeline is very simple as it consists of only two functions: the first 
discoveres the host/process/thread on which it is run and the second prints this
information locally. Computational resources are specified as arguments given to
executalbe script in the form of comma-seperated host:port#number triplets 
i.e.::

    $ python pipeline.py --workers=HOST1:PORT1#2,HOST2:PORT1#4
    
This will fork 2 processes on HOST1:PORT1 and 4 on HOST2:PORT1. This pipeline 
does not use local resources for worker-functions at all.





 
    





  

