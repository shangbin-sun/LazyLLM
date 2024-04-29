from .module import ModuleBase
from lazyllm import OptionIter, ForkProcess
import time
import copy
import atexit
import multiprocessing

def get_options(x):
    if isinstance(x, ModuleBase):
        return x.options
    return []

# TODO(wangzhihong): add process pool to control parallel-number and collect result
class TrialModule(object):
    def __init__(self, m):
        self.m = m

    @staticmethod
    def work(m, q):
        # update option at module.update()
        copy.deepcopy(m).update()
        q.put(m.eval_result)

    def update(self):
        options = get_options(self.m)
        q = multiprocessing.Queue()
        ps = []
        for _ in OptionIter(options, get_options):
            p = ForkProcess(target=TrialModule.work, args=(self.m, q), sync=True)
            ps.append(p)
            p.start()
            time.sleep(1)
        [p.join() for p in ps]
        result = [q.get() for p in ps]
        print(result)