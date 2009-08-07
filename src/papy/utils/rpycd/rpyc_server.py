#!/usr/bin/env python

RPYCD_CONFIG_FILE = 'rpycd_config'
RPYCD_PID_FILE = 'rpycd.pid'

# CONFIG
import sys
import os
from optparse import OptionParser
from ConfigParser import ConfigParser, ParsingError, NoOptionError
from rpyc.utils.classic import DEFAULT_SERVER_PORT
from rpyc.utils.registry import REGISTRY_PORT
from rpyc.utils.registry import UDPRegistryClient, TCPRegistryClient
from rpyc.utils.authenticators import VdbAuthenticator
# RPYC SERVERS
from rpyc.servers.classic_server import serve_forking, serve_threaded, \
                                        serve_stdio
# DAEMON
import daemon

class Options(object):
    def __init__(self):
        # defaults
        self.mode = 'threaded'
        self.port = DEFAULT_SERVER_PORT
        self.host = '0.0.0.0'
        self.logfile = None
        self.quiet = False
        self.vdbfile = None
        self.auto_register = True
        self.regtype = 'udp'
        self.regport = REGISTRY_PORT
        self.reghost = None
        # these are not options
        self.handler = None
        self.registrar = None
        self.authenticator = None

def file_options(file):
    options = Options()
    config = ConfigParser()
    config.read([file])

    # CHANGE DEFAULTS
    for attr in ('mode', 'port', 'host', 'logfile', 'quiet', 'vdbfile', \
                      'auto_register', 'regtype', 'regport', 'reghost'):
        try:
            try:
                opt = config.getboolean('options', attr)
            except ValueError:
                try:
                    opt = config.getint('options', attr)
                except ValueError:
                    opt = config.get('options', attr)
            setattr(options, attr, opt)
        except NoOptionError:
            pass
    return options

def line_options():
    parser = OptionParser()
    parser.add_option("--config", action="store", dest="config", type="str",
    metavar="FILE", default=None, help="specify the configuration file, "
    "default is stderr")
    parser.add_option("-m", "--mode", action="store", dest="mode", metavar="MODE",
    default="threaded", type="string", help="mode can be 'threaded', 'forking', "
    "or 'stdio' to operate over the standard IO pipes (for inetd, etc.). "
    "Default is 'threaded'")
    parser.add_option("-p", "--port", action="store", dest="port", type="int",
    metavar="PORT", default=DEFAULT_SERVER_PORT, help="specify a different "
    "TCP listener port. Default is 18812")
    parser.add_option("--host", action="store", dest="host", type="str",
    metavar="HOST", default="0.0.0.0", help="specify a different "
    "host to bind to. Default is 0.0.0.0")
    parser.add_option("--logfile", action="store", dest="logfile", type="str",
    metavar="FILE", default=None, help="specify the log file to use; the "
    "default is stderr")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
    default=False, help="quiet mode (no logging). in stdio mode, "
    "writes to /dev/null")
    parser.add_option("--vdb", action="store", dest="vdbfile", metavar="FILENAME",
    default=None, help="starts an TLS/SSL authenticated server (using tlslite);"
    "the credentials are loaded from the vdb file. if not given, the server"
    "is not secure (unauthenticated). use vdbconf.py to manage vdb files")
    parser.add_option("--dont-register", action="store_false", dest="auto_register",
    default=True, help="disables this server from registering at all. "
    "By default, the server will attempt to register")
    parser.add_option("--registry-type", action="store", dest="regtype", type="str",
    default="udp", help="can be 'udp' or 'tcp', default is 'udp'")
    parser.add_option("--registry-port", action="store", dest="regport", type="int",
    default=REGISTRY_PORT, help="the UDP/TCP port. default is %s" % (REGISTRY_PORT,))
    parser.add_option("--registry-host", action="store", dest="reghost", type="str",
    default=None, help="the registry host machine. for UDP, the default is "
    "255.255.255.255; for TCP, a value is required")

    options, args = parser.parse_args()
    if args:
        parser.error("does not take positional arguments: %r" % (args,))
    options.mode = options.mode.lower()
    if options.config:
        options = file_options(options.config)
    return options

def validate(options):
    if options.regtype == "udp":
        options.reghost = (options.reghost or "255.255.255.255")
        options.registrar = UDPRegistryClient(ip=options.reghost, port=options.regport)
    elif options.regtype == "tcp":
        if options.reghost:
            options.registrar = TCPRegistryClient(ip=options.reghost, port=options.regport)
        else:
            raise ParsingError("must specific reghost")
    else:
        raise ParsingError("invalid registry type %r" % (options.regtype,))

    if options.vdbfile:
        if not os.path.exists(options.vdbfile):
            ParsingError("vdb file does not exist")
        options.authenticator = VdbAuthenticator.from_file(options.vdbfile)
    else:
        options.authenticator = None

    options.handler = "serve_%s" % (options.mode,)
    print options.handler
    if options.handler not in globals():
        ParsingError("invalid mode %r" % (options.mode,))
    return options

def main():
    pidfile = open(RPYCD_PID_FILE, 'w')
    pidfile.write("%s" % os.getpid())
    pidfile.write("%s" % os.getcwd())
    pidfile.close()
    if len(sys.argv) > 1:
        options = validate(line_options())
    else:
        options = validate(file_options(RPYCD_CONFIG_FILE))
    handler = globals()[options.handler]
    handler(options)

if __name__ == "__main__":
    print sys.argv
    context = daemon.DaemonContext(stdout=sys.stdout,
                                   stderr=sys.stderr,
                                   working_directory=os.getcwd()
                                   )
    with context:
        main()



# EOF
