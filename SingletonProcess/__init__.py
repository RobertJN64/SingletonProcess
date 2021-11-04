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

class ReturnObject:
    """Wrapper for MapResult to pull value from list"""
    def __init__(self, mapResult: MapResult):
        self.raw = mapResult

    def get(self, timeout = None, waitforready=False):
        if not self.raw.ready() and not waitforready:
            raise Exception("Attempting to get result of process that has not finished!")
        return self.raw.get(timeout=timeout)[0] #map returns a list

activepools: Dict[str, List[PIDPool]] = {'default': []}
idcounter = 0

def cleanupDeadProcesses(poolgroup='default', verbose=False):
    """
    Removes all processes that have stoppped

    Returns True if removed at least one value
    """
    for i in range(len(activepools[poolgroup]) - 1, -1, -1):
        if activepools[poolgroup][i].result.ready():
            item = activepools[poolgroup].pop(i)
            if verbose:
                print("Removing terminated process with pid: <" + str(item.pid) + ">")

def block(pid = None, poolgroup = 'default', delay = 0.1, verbose=False):
    """Waits for all processes matching pid in poolgroup to exit, if pid is None, waits for all"""
    while True:
        sleep(delay)
        cleanupDeadProcesses(poolgroup, verbose)
        for item in activepools[poolgroup]:
            if item.pid == pid or pid is None:
                break
        else:
            break #drops out of loop


def terminateProcessesByPID(pid, poolgroup='default', verbose=False):
    """Terminates all processes that match pid, or all if None is provided."""
    cleanupDeadProcesses(poolgroup, verbose)
    for i in range(len(activepools[poolgroup]) -1, -1, -1):
        item = activepools[poolgroup][i]
        if item.pid == pid or pid is None or item.pid is None:
            if verbose:
                if pid is None:
                    reason = "terminating all processes."
                elif item.pid is None:
                    reason = "process did not have pid."
                else:
                    reason = "pid matched with terminate request."
                print("Terminating process with pid: <" + str(item.pid) + "> because", reason)
            item.pool.terminate()
            item.pool.join()
            activepools[poolgroup].pop(i)

class SingletonProcess:
    poolgroup = 'default'
    verbose = False

    def __init__(self, func):
        """Creates a singleton process from a function"""
        self.func = func
        activepools[self.poolgroup] = []

    @staticmethod
    def getPID(args, kwargs):
        """Looks for a pid kwarg in function call. This could be overridden for your use case"""
        _ = args
        if 'pid' in kwargs:
            return kwargs.pop('pid')
        else:
            return None

    def __call__(self, *args, **kwargs):
        """Calls the function with given args, and terminates existing processes with matching ids"""
        global idcounter
        def subwrapper(allargs):
            return self.func(*allargs[0], **allargs[1])

        pid = self.getPID(args, kwargs)
        if self.verbose:
            print("Calling func", self.func.__name__, "with pid: <" + str(pid) + "> in a new process.")
        terminateProcessesByPID(pid, self.poolgroup, self.verbose)

        idcounter += 1
        pool = multiprocessing.ProcessPool(id=idcounter)
        result = pool.amap(subwrapper, [(args, kwargs)])
        activepools[self.poolgroup].append(PIDPool(pid, pool, result))
        return ReturnObject(result)

class VBSingletonProcess(SingletonProcess):
    """Verbose alternative to SingletonProcess, functionally identical"""
    verbose = True