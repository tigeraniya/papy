from IMap import IMap, imports

@imports([['numpy.random', ['random']]])
def make_random():
    while True:
        a = random(10000000)
        yield a

def returner(i):
    return i

if __name__ == "__main__":
    from itertools import repeat
    randoms = make_random()
    imap_ = IMap(returner, randoms)
    for i in imap_:
        print i



