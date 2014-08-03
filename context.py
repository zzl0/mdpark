# coding: utf-8

from rdd import *
from schedule import *
from env import env


class MDparkContext:
    nextRddId = 0
    nextShuffleId = 0

    def __init__(self, master='local'):
        self.master = master
        self.init()

    def init(self):
        env.create(True)
        self.env = env

        master = self.master
        if master == 'local':
            self.scheduler = LocalScheduler()
            self.isLocal = True
        elif master == 'process':
            self.scheduler = MultiProcessScheduler(2)
            self.isLocal = False
        else:
            # TODO: mesos
            raise Exception('to do mesos scheduler')

        self.defaultParallelism = self.scheduler.defaultParallelism
        self.defaultMinSplits = max(self.defaultParallelism, 2)
        self.scheduler.start()

    def newRddId(self):
        self.nextRddId += 1
        return self.nextRddId

    def newShuffleId(self):
        self.nextShuffleId += 1
        return self.nextShuffleId

    def parallelize(self, seq, numSlices=None):
        if numSlices is None:
            numSlices = self.defaultParallelism  #? why not use self.defaultMinSplits
        return ParallelCollection(self, seq, numSlices)

    def makeRDD(self, seq, numSlices=None):
        return self.parallelize(seq, numSlices)

    def textFile(self, path, numSplits=None, splitSize=None, ext=''):
        if not os.path.exists(path):
            raise IOError("%s not exists" % path)

        if os.path.isdir(path):
            rdds = [TextFileRDD(self, os.path.join(path, n), numSplits, splitSize)
                    for n in os.listdir(path)
                    if not os.path.isdir(os.path.join(path, n)) and n.endswith(ext)]
            return self.union(rdds)
        else:
            return TextFileRDD(self, path, numSplits, splitSize)

    def union(self, rdds):
        return UnionRDD(self, rdds)

    def stop(self):
        self.scheduler.stop()

    def runJob(self, rdd, func, partitions=None, allowLocal=False):
        if partitions is None:
            partitions = range(len(rdd.splits))
        return self.scheduler.runJob(rdd, lambda _, it: func(it), partitions, allowLocal)

    def __getstate__(self):
        return self.master

    def __setstate__(self, state):
        self.master = state
