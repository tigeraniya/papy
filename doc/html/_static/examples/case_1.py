# This pipeline calls a remote WSDL service using a provided perl script
# of course your perl has to have the required modules!

# what is the name of your perl? 
PERL = 'perl'

#0. required imports
from papy import *
from IMap import IMap

#
utils.logger.start_logger(log_to_screen=False, log_rotate =True)

#1. functions definitions
@imports([['subprocess',[]], ['tempfile', []]])
def call_dalilite(inbox, email):
    """ Call dalilite
    """
    ids = inbox[0]
    fd, name = tempfile.mkstemp(suffix ='.zip')
    perl = subprocess.Popen([PERL, 'dalilite.pl',
                                   '--email', email,\
                                   '--pdb1', ids[0],\
                                   '--pdb2', ids[1],\
                                   '--outfile', name[:-4]],
                                   stdout =subprocess.PIPE,\
                                   stderr =subprocess.PIPE
                                   )
    perl.stdout.read() # this will block until the result is ready
    perl.stderr.read()
    return name

@imports([['zipfile',[]], ['re', []]]) 
def get_rmsds(inbox):
    name = inbox[0]
    zip = zipfile.PyZipFile(name)
    matrix = zip.read('matrix.txt')
    result = {}
    pattern = re.compile('Query\s=\smol(\S{2}),\sSbjct\s=\smol(\S{2})')
    for line in matrix.splitlines():
        if line.startswith('# Alignment'):
            id1, id2 = pattern.search(line).groups()
            rmsd = float(line[-12:-9])
            result["_".join((id1, id2))] = rmsd
    return result


#2. parallelism and topology of pipeline
def pipeline(Imap):
    # wrap the functions into workers and pipers.
    dali_worker = Worker(call_dalilite, ('mpc4p@virginia.edu',))
    dali_piper = Piper(dali_worker, parallel =Imap, debug =True)
    rmsd_piper = Piper(get_rmsds, debug =True)
    # the next piper uses a builtin worker function
    print_piper = Piper(workers.io.print_, debug =True)
    # define the topology
    pipes = Plumber()
    pipes.add_pipe((dali_piper, rmsd_piper, print_piper))
    return pipes


#3. run-time
if __name__ == '__main__':
    # run it twice in as parallel thread and linear
    for imap_ in (IMap(worker_type ='thread'),):
        # make input input 
        ids = iter([('1mgq', '1i8f'), ('1w21', '1l7f')])
        #get pipeline instance
        pipes = pipeline(imap_)
        # connect input data
        pipes.set_inputs([ids])
        # start calculations/processing
        pipes.plunge()
        # wait until it is finished
        pipes._is_finished.wait() 
        print "Calculation using %s took %fs" % (imap_, pipes.stats['run_time'])

