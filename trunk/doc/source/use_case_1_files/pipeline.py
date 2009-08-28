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
logger.start_logger(log_rotate=False)

LOOP_NUM = 10


# Part 1: Define user functions
@imports(['MMTK.PDB'])
def create_model(inbox, strip):
    """
    Arguments:
    
        * strip(bool)
        
            Remove all but protein and water?
    """
    filename = inbox[0]
    return True

@imports(['MMTK', 'MMTK.ForceFields', 'MMTK.Minimization'])
def minimize_model(inbox, steps):
    model = inbox[0]
    return model

@imports(['MMTK', 'MMTK.Dynamics', 'MMTK.Trajectory'])
def equilibrate_model(inbox, steps, t_start, t_stop, t_step):
    model = inbox[0]
    return model

@imports(['MMTK.PDB'])
def save_model(inbox, formats, temp):
    model = inbox[0]
    filename = 'dummy_file'
    return filename

@imports(['os'])
def call_stride(inbox, temp):
    filename = inbox[0]
    results = 'dummy_results'
    return results

def define_loops(inbox, min_size, max_gaps):
    stride_results = inbox[0]
    loops = []
    return loops

@imports(['MMTK'])
def create_loop_models(inbox, loop_num):
    model = inbox[0]
    loop_models = []
    for i in xrange(loop_num - len(loop_models)):
        loop_models.append(None)
    return loop_models

@imports(['MMTK', 'MMTK.Dynamics', 'MMTK.Trajectory'])
def md_loop_model(inbox, steps, temp, save):
    loop_model = inbox[0]
    if loop_model:
        pass
    return loop_model

@imports(['MMTK'])
def combine_models(inbox):
    models = inbox[0]
    model = 'dummy_model'
    return model


# Part 2: Define the topology
def pipeline():
    pool = IMap()
    pipes = Plumber()

    # initialize Worker instances (i.e. wrap the functions).
    w_create_model = Worker(create_model, kwargs={
                                                  'strip': True
                                                  })
    w_minimize_model = Worker(minimize_model, kwargs={
                                                      'steps': 100
                                                      })
    w_equilibrate_model = Worker(equilibrate_model, kwargs={
                                                            'steps':1000,
                                                            't_start':50, # K
                                                            't_stop':300, # K
                                                            't_step':0.5 # K
                                                            })
    w_save_model = Worker(save_model, kwargs={
                                              'formats':['pdb'],
                                              'temp': True
                                              })
    w_call_stride = Worker(call_stride, kwargs={'temp': True})
    w_define_loops = Worker(define_loops, kwargs={
                                                  'min_size':7,
                                                  'max_gaps':2
                                                  })
    w_create_loop_models = Worker(create_loop_models, kwargs={
                                                               'loop_num':LOOP_NUM
                                                               })
    w_md_loop_model = Worker(md_loop_model, kwargs={
                                                     'steps':10000,
                                                     'temp':300, # K 
                                                     'save':False
                                                     })
    w_combine_models = Worker(combine_models)
    w_save_final_model = Worker(save_model, kwargs={
                                                    'formats':['mmtk', 'pdb'],
                                                    'temp':False
                                                    })
    # initialize Piper instances (i.e. attach functions to runtime)
    p_create_model = Piper(w_create_model, debug=True)
    p_minimize_model = Piper(w_minimize_model, debug=True)
    p_equilibrate_model = Piper(w_equilibrate_model, debug=True)
    p_save_model = Piper(w_save_model, debug=True)
    P_call_stride = Piper(w_call_stride, debug=True)
    p_define_loops = Piper(w_define_loops, debug=True)
    p_create_loop_models = Piper(w_create_loop_models, debug=True)
    p_md_loop_model = Piper(w_md_loop_model, debug=True)
    p_combine_models = Piper(w_combine_models, debug=True)
    p_save_final_model = Piper(w_save_final_model, debug=True)

    # create the pipeline and connect pipers
    pipes.add_pipe((
                    p_create_model,
                    p_minimize_model,
                    p_equilibrate_model,
                    p_create_loop_models,
                    p_md_loop_model,
                    p_combine_models,
                    p_save_final_model
                    ))
#    pipes.add_pipe((
#                    p_equilibrate_model,
#                    p_save_model,
#                    P_call_stride,
#                    p_define_loops,
#                    p_create_loop_models
#                    ))
    return pipes


# Part 3: execute the pipeline
if __name__ == '__main__':
    pipes = pipeline()
    pipes.start([range(10)])
    print 'started'
    pipes.run()
    print 'running'
    pipes.wait()
    print 'finished'
    pipes.pause()
    pipes.stop()
    print pipes.stats


