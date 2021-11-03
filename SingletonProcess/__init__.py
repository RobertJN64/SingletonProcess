import pathos.multiprocessing as multiprocessing
from typing import List, Dict
from multiprocessing.pool import MapResult
import dataclasses
from time import sleep

@dataclasses.dataclass
class PIDPool:
    """Used for tracking all active processes"""
    pid: str
    pool: multiprocessing.ProcessPool
    result: MapResult

activepools: Dict[str, List[PIDPool]] = {'default': []}
idcounter = 0

def cleanupDeadProcesses(poolgroup='default'):
    """
    Removes all processes that have stoppped

    Returns True if removed at least one value
    """
    for i in range(len(activepools[poolgroup]) - 1, -1, -1):
        if activepools[poolgroup][i].result.ready():
            activepools[poolgroup].pop(i)


def block(pid = None, poolgroup = 'default', delay=1):
    """Waits for all processes matching pid to exit, if pid is None, waits for all"""
    while True:
        sleep(delay)
        cleanupDeadProcesses()
        for item in activepools[poolgroup]:
            if item.pid == pid or pid is None:
                break
        else:
            break #drops out of loop


def terminateProcessesByPID(pid, poolgroup):
    """Terminates all processes that match pid, or all if None is provided."""
    for i in range(len(activepools[poolgroup]) -1, -1, -1):
        item = activepools[poolgroup][i]
        if item.pid == pid or pid is None or item.pid is None:
            item.pool.terminate()
            item.pool.join()
            activepools[poolgroup].pop(i)

class SingletonProcess:
    poolgroup = 'default'

    def __init__(self, func):
        """Creates a singleton process from a function"""
        self.func = func

    @staticmethod
    def getPID(args, kwargs):
        """Looks for a pid kwarg in function call. This could be overridden for your use case"""
        _ = args
        if 'pid' in kwargs:
            retval = kwargs['pid']
            kwargs.pop('pid')
            return retval
        else:
            return None

    def __call__(self, *args, **kwargs):
        """Calls the function with given args, and terminates existing processes with matching ids"""
        global idcounter
        def subwrapper(allargs):
            self.func(*allargs[0], **allargs[1])

        pid = self.getPID(args, kwargs)
        terminateProcessesByPID(pid, self.poolgroup)

        idcounter += 1
        pool = multiprocessing.ProcessPool(id=idcounter)
        result = pool.amap(subwrapper, [(args, kwargs)])
        activepools[self.poolgroup].append(PIDPool(pid, pool, result))