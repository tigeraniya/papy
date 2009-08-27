"""
Template string for saving *PaPy* pipelines as Python source code.
"""
# imap call signature
I_SIG = '    %s = IMap(worker_type="%s", worker_num=%s, stride=%s, buffer=%s, ' + \
                  'ordered =%s, skip =%s, name ="%s")\n'

# piper call signature
P_SIG = '    %s = Piper(%s, parallel=%s, consume=%s, produce=%s, spawn=%s, ' + \
                            'timeout=%s, ' + \
                            'cmp=%s, ornament=%s, debug=%s, name="%s", ' + \
                            'track=%s)\n'
# worker call signature
W_SIG = 'Worker((%s,), %s, %s)'
# list signature
L_SIG = '(%s, %s)'
# papy pipeline source-file layout
P_LAY = \
    'from papy import *' + '\n' + \
    'from IMap import IMap' + '\n\n' + \
    '%s' + '\n' + \
    '%s' + '\n\n' + \
    'def pipeline():' + '\n' + \
             '%s' + '\n\n' + \
             '%s' + '\n\n' + \
    '    ' + 'pipers = %s' + '\n' + \
    '    ' + 'xtras = %s' + '\n' + \
    '    ' + 'pipes  = %s' + '\n' + \
    '    ' + 'return (pipers, xtras, pipes)' + '\n\n' + \
    'if __name__ == "__main__":' + '\n' + \
    '    ' + 'pipeline()' + '\n' + \
    '' + '\n'
