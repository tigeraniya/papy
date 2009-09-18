#!/usr/bin/env python
"""
"""
# Part 0: import the PaPy infrastructure.
# interface of the API: 
from papy import Plumber, Piper, Worker
# the parallel IMap function and imports wrapper: 
from IMap import IMap, imports
# all example workers
from papy import workers
# logging support
import logging
from papy.utils import logger
# 
import sys
sys.stderr = open('/dev/null', 'w')
LOOP_NUM = 10 # maximum number of loops

# Part 1: Define user functions
@imports(['re', 'StringIO.StringIO'])
def create_dummy_files(input_file):
    handle = open(input_file)
    match_content = re.compile('<content>(.*?)</content>.*?UP (.*?)\s+\d', re.DOTALL)
    file_strings = match_content.finditer(handle.read())
    model = 0
    while True:
        file_content, model_name = file_strings.next().groups()
        yield (StringIO(file_content), model_name)
        model += 1
        if model == 2:
            raise StopIteration

@imports(['MMTK', 'MMTK.PDB', 'MMTK.Proteins', 'MMTK.ForceFields'])
def create_model(inbox, forcefield, save_file):
    dummy_file, model_name = inbox[0]
    print 'create_model: %s' % model_name
    # create the protein
    configuration = PDB.PDBConfiguration(dummy_file)
    chains = configuration.createPeptideChains()
    protein = Proteins.Protein(chains)
    # create the forcefield
    if forcefield == 'amber94':
        forcefield = ForceFields.Amber94ForceField()
    elif forcefield == 'amber99':
        forcefield = ForceFields.Amber99ForceField()
    # create and fill the univers
    universe = MMTK.InfiniteUniverse(name=model_name)
    universe.setForceField(forcefield)
    universe.protein = protein
    if save_file:
        universe.protein.writeToFile('results/%s_initial.pdb' % universe.name)
    return universe

@imports(['MMTK.Trajectory', 'MMTK.Minimization'])
def minimize_model(inbox, steps, convergence, save_file, save_log):
    universe = inbox[0]
    print 'minimize_model: %s' % universe.name
    actions = []
    if save_log:
        actions.append(
         Trajectory.LogOutput('results/%s_minimization.log' % universe.name))
    minimizer = Minimization.ConjugateGradientMinimizer(universe, actions=actions)

    minimizer(convergence=convergence, steps=steps)
    if save_file:
        universe.protein.writeToFile('results/%s_minimized.pdb' % universe.name)
    return universe

@imports(['MMTK', 'MMTK.Dynamics', 'MMTK.Trajectory'])
def equilibrate_model(inbox, steps, T_start, T_stop, T_step, save_file, save_log):
    universe = inbox[0]
    print 'equilibrate_model: %s' % universe.name
    universe.initializeVelocitiesToTemperature(T_start * MMTK.Units.K)

    # Create integrator
    integrator = Dynamics.VelocityVerletIntegrator(universe,
                                                   delta_t=1. * MMTK.Units.fs)
    actions = [
        # Heat from t_start K to t_stop K applying a temperature
        # change of t_step K/fs; scale velocities at every step.
        Dynamics.Heater(T_start * MMTK.Units.K,
                        T_stop * MMTK.Units.K,
                        T_step * MMTK.Units.K / MMTK.Units.fs,
                        0, None, 1),
        # Remove global translation every 50 steps.
        Dynamics.TranslationRemover(0, None, 50),
        # Remove global rotation every 50 steps.
        Dynamics.RotationRemover(0, None, 50)]
    if save_log:
        # Log output to file.
        actions.append(Trajectory.LogOutput('results/%s_equilibration.log' % \
                                            universe.name))
    integrator(steps=steps, actions=actions)
    if save_file:
        universe.protein.writeToFile('results/%s_equilibrated.pdb' % universe.name)
    return universe

@imports(['subprocess'])
def call_stride(inbox):
    universe = inbox[0]
    print 'call_stride on model: %s' % universe.name
    filename = "results/%s_equilibrated.pdb" % universe.name
    process = subprocess.Popen('stride %s' % filename, shell=True,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE)
    output = []
    # here the STRIDE output file is parsed for the relevant data.
    # Lines beginning with ASG contain per-residue sec. structure assignments.
    for line in process.stdout.xreadlines():
        if line.startswith('ASG'):
            res_name = line[5:8] # we use 3
            chain_id = line[9]
            if chain_id == '-': # stride ' ' -> '-' rename
                chain_id = ' '
            try:
                res_id = int(float(line[10:15]))
                res_ic = ' '
            except ValueError:
                res_id = float(line[10:14])
                res_ic = line[14]
            ss_code = line[24]
            phi = float(line[43:49])
            psi = float(line[53:59])
            asa = float(line[62:69])
            output.append((res_id, ss_code))
            # output[(chain_id, (res_name, res_id, res_ic))] = 
            # (ss_code, phi, psi, asa)
    if not output:
        # this propably means a Calpha only chain has been supplied
        raise RuntimeError
    return output

def define_loops(inbox, min_size, max_gaps):
    print 'define loops'
    stride_results = inbox[0]
    loops = []
    new = True
    for res_id, ss_code in stride_results:
        if ss_code in ('E', 'H'):
            new = True
            continue
        else:
            if new:
                loops.append([])
            new = False
            loops[-1].append(res_id)
    return loops

@imports(['MMTK', 'MMTK.Proteins', 'MMTK.PDB', 'os'])
def create_loop_models(inbox, loop_num, sphere_margin, save_file):
    loops, universe = inbox
    print 'create loop models: %s' % universe.name, loops
    residues = universe.protein.residues()
    loop_models = []
    try:
        for i, loop in enumerate(loops):
            # figure out which residues belong to loop
            loop_res = MMTK.Collections.Collection()
            for res_id in loop:
                loop_res.addObject(residues[res_id - 1])
            # determine a bounding sphere for the residues
            bs = loop_res.boundingSphere()
            # select all residues in protein within the sphere plus margin
            loop_sphere = residues.selectShell(bs.center, bs.radius + sphere_margin)
            # determine the offset of the loop
            offset_protein = loop[0] - 1
            offset_loop = list(loop_sphere).index(residues[offset_protein])
            ###### create new universe
            # save the sphere around the loop as a new file
            loop_name = "%s_loop%s" % (universe.name, i)
            loop_file = 'results/%s_equilibrated.pdb' % loop_name
            pdb_file = PDB.PDBOutputFile(loop_file)
            pdb_file.write(loop_sphere)
            pdb_file.close()
            # load the new file as if it was a proper chain (the loop is proper)
            # MMTK writes terminal forms of residues 
            configuration = PDB.PDBConfiguration(loop_file)
            chain = Proteins.PeptideChain(configuration.peptide_chains[0],
                                  n_terminus=loop_sphere[0] == residues[0],
                                  c_terminus=loop_sphere[-1] == residues[-1]) # not sure about that
            protein = Proteins.Protein(chain)
            loop_universe = MMTK.InfiniteUniverse(name=loop_name)
            loop_universe.setForceField(universe.forcefield())
            loop_universe.protein = protein
            loop_residues = loop_universe.protein.residues()
            # now fix all atoms which are not the initial loop
            # select real loop residues
            left_residues = loop_residues[0:offset_loop]
            right_residues = loop_residues[offset_loop + len(loop):]
            #print 'loop: %s' % (i + 1,)
            #print 'residues in loop: %s' % list(residues[offset_protein:offset_protein + len(loop)])
            #print 'residues in shell: %s' % list(loop_sphere)
            #print 'residues to be fixed: %s' % (left_residues + right_residues)
            for residue in left_residues + right_residues:
                for atom in residue.atomList():
                    atom.fixed = True
            loop_models.append((loop_universe, (offset_loop, offset_protein, len(loop))))
            if not save_file:
                os.unlink(loop_file)
    except Exception, e:
        print i, loop, list(loop_sphere), e
        raise
    print 'created loop models: %s' % loop_models
    for i in xrange(loop_num - len(loop_models)):
        loop_models.append(None)
    return loop_models

@imports(['MMTK', 'MMTK.Dynamics', 'MMTK.Trajectory'])
def md_loop_model(inbox, steps, temp, save_file, save_trajectory, save_log):
    # this is for produce/spawn/consume padding
    if inbox[0] is None:
        return None
    loop_universe = inbox[0][0]
    print 'md of loop model: %s' % loop_universe.name
    actions = []
    if save_log:
        actions.append(Trajectory.LogOutput('results/%s_refinement.log' % \
                                            loop_universe.name))
    if save_trajectory:
        traj = Trajectory.Trajectory(loop_universe, "%s.nc" % loop_universe.name, "w")
        # Write every second step to the trajectory file.
        actions.append(Trajectory.TrajectoryOutput(traj, \
                        ("time", "energy", "thermodynamic", "configuration"),
                        0, None, 2))

    loop_universe.initializeVelocitiesToTemperature(temp * MMTK.Units.K)
    integrator = Dynamics.VelocityVerletIntegrator(loop_universe, delta_t=1. * MMTK.Units.fs)
    integrator(steps=steps, actions=actions)
    if save_trajectory:
        traj.close()
    if save_file:
        loop_universe.protein.writeToFile('results/%s_refined.pdb' % loop_universe.name)
    return inbox[0]

def combine_loop_models(inboxes):
    print 'collect loop models model'
    loop_universes_offsets = [i[0] for i in inboxes if i[0] is not None]
    return loop_universes_offsets

@imports(['itertools'])
def make_refined_model(inbox, save_file):
    combined, initial = inbox
    print 'make refined model: %s' % initial.name
    residues = initial.protein[0].residues() # residues in the first peptide chain
    for loop_universe, (offset_loop, offset_protein, len_loop) in combined:
        initial_residues = residues[offset_protein:offset_protein + len_loop]
        refined_residues = loop_universe.protein[0].residues()[offset_loop:offset_loop + len_loop]
        for ir, rr in itertools.izip(initial_residues, refined_residues):
            for ia, ra in itertools.izip(ir.atomList(), rr.atomList()):
                ia.setPosition(ra.position())
    if save_file:
        initial.protein.writeToFile('results/%s_refined.pdb' % initial.name)



# Part 2: Define the topology
def pipeline():
    pool = IMap(worker_num=2, buffer=100)
    pipes = Plumber(logger_options ={
                                     'log_to_screen':True,
                                     'log_to_screen_level':logging.INFOR   
                                     })

    # initialize Worker instances (i.e. wrap the functions).
    w_create_model = Worker(create_model, kwargs={
                                                  'forcefield': 'amber99',
                                                  'save_file':True
                                                  })
    w_minimize_model = Worker(minimize_model, kwargs={
                                                      'steps': 100,
                                                      'convergence':1.0e-4,
                                                      'save_log':True,
                                                      'save_file':True
                                                      })
    w_equilibrate_model = Worker(equilibrate_model, kwargs={
                        # 50K -> 300K in 500 0.5K steps
                                                            'steps':500, 
                                                            't_start':50., # K
                                                            't_stop':300., # K
                                                            't_step':0.5, # K
                                                            'save_log':True,
                                                            'save_file':True
                                                            })
    w_minimize_equilibrate_model = Worker((w_minimize_model, w_equilibrate_model))
    w_call_stride = Worker(call_stride)
    w_define_loops = Worker(define_loops)
    w_create_loop_models = Worker(create_loop_models, kwargs={
                                                              'sphere_margin':0.5, # nm
                                                              'loop_num':LOOP_NUM,
                                                              'save_file':True
                                                              })
    w_md_loop_model = Worker(md_loop_model, kwargs={
                                                    'steps':50000,
                                                    'temp':300, # K 
                                                    'save_file':True,
                                                    'save_trajectory':False,
                                                    'save_log':True
                                                    })
    w_combine_loop_models = Worker(combine_loop_models)
    w_make_refined_model = Worker(make_refined_model, kwargs={
                                                               'save_file':True
                                                               })

    # initialize Piper instances (i.e. attach functions to runtime)
    p_create_model = Piper(w_create_model, debug=True)
    p_equilibrate_model = Piper(w_minimize_equilibrate_model, parallel=pool, debug=True)
    P_call_stride = Piper(w_call_stride, debug=True)
    p_define_loops = Piper(w_define_loops, debug=True)
    p_create_loop_models = Piper(w_create_loop_models, debug=False, produce=LOOP_NUM)
    p_md_loop_model = Piper(w_md_loop_model, debug=True, parallel=pool, spawn=LOOP_NUM)
    p_combine_loop_models = Piper(w_combine_loop_models, debug=True, consume=LOOP_NUM)
    p_make_refined_model = Piper(w_make_refined_model, debug=True)

    # create the pipeline and connect pipers
    pipes.add_pipe((
                    p_create_model,
                    p_equilibrate_model,
                    p_create_loop_models,
                    p_md_loop_model,
                    p_combine_loop_models,
                    p_make_refined_model
                    ))  # main branch
    pipes.add_pipe((
                    p_equilibrate_model,
                    P_call_stride,
                    p_define_loops,
                    p_create_loop_models
                    ))  # stride branch
    pipes.add_pipe((
                    p_equilibrate_model,
                    p_make_refined_model
                    ))  # data-link branch
    return pipes


# Part 3: execute the pipeline
if __name__ == '__main__':
    pipes = pipeline()
    pipes.start([create_dummy_files('data/hfq_models.xml')])
    pipes.run()
    pipes.wait()
    pipes.pause()
    pipes.stop()
    print pipes.stats


