#0. required imports
from papy import *
from IMap import IMap


#1. functions definitions
@imports([['MMTK.Trajectory', ['Trajectory']]])
def arrays_from_netcdf(file_name):
    trajectory = Trajectory.Trajectory(None, file_name)
    steps = len(trajectory)
    for i in xrange(steps):
        for j in xrange(steps):
            if (i + j) > (steps -2):
                continue
            else:
                yield (trajectory[i]['configuration'].array,\
                       trajectory[j]['configuration'].array,)

@imports([['numpy', ['mean','sum', 'power', 'linalg', 'dot', 'sqrt']]])
def calc_rmsd(inbox):
    for i in range(1):
        coords_1, coords_2 = inbox[0]
        assert len(coords_1) == len(coords_2) > 0
        coords_1 = coords_1 - mean(coords_1, axis =0)   # centers matrices to
        coords_2 = coords_2 - mean(coords_2, axis =0)   # avoid affine transformation
        # Initial residual, see Kabsch.
        E0 = sum(power(coords_1, 2)) + sum(power(coords_2, 2))
        V, S, Wt = linalg.svd(dot(coords_2.transpose(), coords_1))
        reflect = float(str(float(linalg.det(V) * linalg.det(Wt))))
        if reflect == -1.0:
                S[-1] = -S[-1]
                V[:,-1] = -V[:,-1]
        RMSD = E0 - (2.0 * sum(S))
        RMSD = sqrt(abs(RMSD / len(coords_1)))
        U = dot(V, Wt)               # rotation matrix
    return (U, RMSD)


#2. parallelism and topology of pipeline
def pipeline(Imap):
    # wrap the functions into workers and pipers.
    rmsd_piper = Piper(calc_rmsd, parallel =Imap, debug =True)
    # the next piper uses a builtin worker function
    print_piper = Piper(workers.io.print_, debug =True)
    # define the topology
    pipes = Plumber()
    pipes.add_piper(rmsd_piper)
    #pipes.add_pipe((rmsd_piper, print_piper))
    return pipes


#3. run-time
if __name__ == '__main__':
    # run it twice in as parallel thread and linear
    for imap_ in (None, IMap(worker_type ='thread'), IMap()):
        # make input input
        arrays = arrays_from_netcdf("3bw1.nc")
        #get pipeline instance
        pipes = pipeline(imap_)
        # connect input data
        pipes.set_inputs([arrays])
        # start calculations/processing
        pipes.plunge()
        # wait until it is finished
        pipes._is_finished.wait() 
        print "Calculation using %s took %fs" % (imap_, pipes.stats['run_time'])

