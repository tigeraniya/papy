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
from papy.utils import logger
logger.start_logger(log_to_file=False, log_to_stream=True,)
import sys
sys.stderr = open('/dev/null', 'w')
LOOP_NUM = 10 # maximum number of loops

# Part 1: Define user functions
@imports(['re', 'StringIO.StringIO'])
def create_dummy_files(input_file):
    handle = open(input_file)
    match_content = re.compile('<content>(.*?)</content>.*?UP (.*?)\s+\d', re.DOTALL)
    file_strings = match_content.finditer(handle.read())
    while True:
        file_content, model_name = file_strings.next().groups()
        yield (StringIO(file_content), model_name)
        break

@imports(['MMTK', 'MMTK.PDB', 'MMTK.Proteins', 'MMTK.ForceFields'])
def create_model(inbox, forcefield, save_file):
    dummy_file, model_name = inbox[0]
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
def equilibrate_model(inbox, steps, t_start, t_stop, t_step, save_file, save_log):
    universe = inbox[0]
    universe.initializeVelocitiesToTemperature(t_start * MMTK.Units.K)

    # Create integrator
    integrator = Dynamics.VelocityVerletIntegrator(universe,
                                                   delta_t=1. * MMTK.Units.fs)
    actions = [
        # Heat from t_start K to t_stop K applying a temperature
        # change of t_step K/fs; scale velocities at every step.
        Dynamics.Heater(t_start * MMTK.Units.K,
                        t_stop * MMTK.Units.K,
                        t_step * MMTK.Units.K / MMTK.Units.fs,
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
    filename = "results/%s_equilibrated.pdb" % universe.name
    process = subprocess.Popen('stride %s' % filename, shell=True,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE)
    output = []
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

@imports(['MMTK'])
def create_loop_models(inbox, loop_num, sphere_margin, save_file):
    loops, universe = inbox
    residues = universe.protein.residues()
    loop_models = []
    for i, loop in enumerate(loops):
        # figure out which residues belong to loop
        loop_res = MMTK.Collections.Collection()
        for res_id in loop:
            loop_res.addObject(residues[res_id - 1])
        loop_res_names = [r.name for r in loop_res]
        # determine a bounding sphere for the residues
        bs = loop_res.boundingSphere()
        # select all residues in protein within the sphere plus margin
        loop_sphere = residues.selectShell(bs.center, bs.radius + sphere_margin)
        # create new universe
        loop_universe = MMTK.InfiniteUniverse(name="%s_loop%s" % (universe.name, i))
        loop_universe.setForceField(universe.forcefield())
        loop_sphere = MMTK.deepcopy(loop_sphere)
        loop_universe.addObject(loop_sphere, steal=True)
        # fix all atoms which are not the refined loop
        for residue in loop_universe:
            if not residue.name in loop_res_names:
                for atom in residue.atomList():
                    atom.fixed = True
        loop_models.append(loop_universe)
        if save_file:
            try:
                loop_universe.writeToFile('results/%s_equilibrated.pdb' % loop_universe.name)
            except Exception, e:
                print e
    for i in xrange(loop_num - len(loop_models)):
        loop_models.append(None)
    return loop_models

@imports(['MMTK', 'MMTK.Dynamics', 'MMTK.Trajectory'])
def md_loop_model(inbox, steps, temp, save):
    loop_model = inbox[0]
    print loop_model
    if loop_model:
        pass
    return loop_model

@imports(['MMTK'])
def combine_models(inboxes):
    models = [inbox[0] for inbox in inboxes]
    model = 'dummy_model'
    return model


# Part 2: Define the topology
def pipeline():
    pool = IMap()
    pipes = Plumber()

    # initialize Worker instances (i.e. wrap the functions).
    w_create_model = Worker(create_model, kwargs={
                                                  'forcefield': 'amber99',
                                                  'save_file':True
                                                  })
    w_minimize_model = Worker(minimize_model, kwargs={
                                                      'steps': 5,
                                                      'convergence':1.0e-4,
                                                      'save_log':True,
                                                      'save_file':True
                                                      })
    w_equilibrate_model = Worker(equilibrate_model, kwargs={
                                                            'steps':5,
                                                            't_start':50., # K
                                                            't_stop':300., # K
                                                            't_step':0.5, # K
                                                            'save_log':True,
                                                            'save_file':True
                                                            })
    w_call_stride = Worker(call_stride)
    w_define_loops = Worker(define_loops, kwargs={
                                                  'min_size':7,
                                                  'max_gaps':2
                                                  })
    w_create_loop_models = Worker(create_loop_models, kwargs={
                                                              'sphere_margin':0.5, # nm
                                                              'loop_num':LOOP_NUM,
                                                              'save_file':True
                                                              })
    w_md_loop_model = Worker(md_loop_model, kwargs={
                                                    'steps':50,
                                                    'temp':300, # K 
                                                    'save':False
                                                    })
    w_combine_models = Worker(combine_models)

    # initialize Piper instances (i.e. attach functions to runtime)
    p_create_model = Piper(w_create_model, debug=True)
    p_minimize_model = Piper(w_minimize_model, parallel=pool, debug=True)
    p_equilibrate_model = Piper(w_equilibrate_model, parallel=pool, debug=True)
    P_call_stride = Piper(w_call_stride, debug=True)
    p_define_loops = Piper(w_define_loops, debug=True)
    p_create_loop_models = Piper(w_create_loop_models, debug=True, produce=LOOP_NUM)
    p_md_loop_model = Piper(w_md_loop_model, debug=True, parallel=pool, spawn=LOOP_NUM)
    p_combine_models = Piper(w_combine_models, debug=True, consume=LOOP_NUM)

    # create the pipeline and connect pipers
    pipes.add_pipe((
                    p_create_model,
                    p_minimize_model,
                    p_equilibrate_model,
                    p_create_loop_models,
                    p_md_loop_model,
                    p_combine_models
                    ))
    pipes.add_pipe((
                    p_equilibrate_model,
                    P_call_stride,
                    p_define_loops,
                    p_create_loop_models
                    ))
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


