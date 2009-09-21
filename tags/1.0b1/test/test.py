from papy import *
from IMap import IMap

from papy.workers.maths import mul

def power(i):
    return i[0] * i[0]
@imports(['sys'])
def sys_ver(i):
    return sys.version


def pipeline():
    illo = IMap(worker_type="process", worker_num=2, stride=2, buffer=None, ordered =True, skip =False, name ="illo")
    imap_139735419033360 = IMap(worker_type="process", worker_num=2, stride=2, buffer=None, ordered =True, skip =False, name ="imap_139735419033360")


    piper_139735419034064 = Piper(Worker((mul,), ((),), ({},)), parallel=False, consume=1, produce=1, spawn=1, produce_from_sequence=False, timeout=None, cmp=None, ornament=None, debug=False, name="piper_139735419034064", track=False)
    piper_139735419033808 = Piper(Worker((power,), ((),), ({},)), parallel=False, consume=1, produce=1, spawn=1, produce_from_sequence=False, timeout=None, cmp=None, ornament=None, debug=False, name="piper_139735419033808", track=False)
    piper_139735418874896 = Piper(Worker((sys_ver,), ((),), ({},)), parallel=illo, consume=1, produce=1, spawn=1, produce_from_sequence=False, timeout=None, cmp=None, ornament=None, debug=False, name="piper_139735418874896", track=False)
    piper_139735419085584 = Piper(Worker((power,), ((),), ({},)), parallel=imap_139735419033360, consume=1, produce=1, spawn=1, produce_from_sequence=False, timeout=None, cmp=None, ornament=None, debug=False, name="piper_139735419085584", track=False)


    pipers = [piper_139735419034064, piper_139735419033808, piper_139735418874896, piper_139735419085584]
    xtras = [{'color': 'red'},{},{},{}]
    pipes  = [(piper_139735419034064, piper_139735419033808)]
    return (pipers, xtras, pipes)

if __name__ == "__main__":
    pipeline()

