import os
from utils import load_func, dump_func
from dependency import (
    OneToOneDependency, ShuffleDependency, Aggregator, HashPartitioner,
)

import logging
logger = logging.getLogger(__file__)


class Split:
    def __init__(self, idx):
        self.index = idx


class RDD:
    """ A Resilient Distributed Dataset (RDD), the basic abstraction in MDpark.

    Each RDD is characterized by five main properties:
    - A list of splits (partitions)
    - A function for computing each split
    - A list of dependencies on other RDDS
    - Optionally, a Partitioner for key-value RDDs (e.g hash-partitioned)
    - Optionally, a list of prefered locations to compute each split on

    All the scheduling and execution in MDpark is done based on these methods,
    allowing each RDD to implement its own way of computing itself.

    This class also contains transformation methods avaible on all RDDs(e.g.
    map and filter).
    """
    def __init__(self, ctx):
        self.ctx = ctx
        self.id = ctx.newRddId()
        self.partitioner = None
        self.shouldCache = False
        self._splits = []

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<%s>" % self.__class__.__name__

    @property
    def splits(self):
        return self._splits

    def compute(self, split):
        raise NotImplementedError

    @property
    def dependencies(self):
        return []

    def preferredLocations(self, split):
        return []

    def iterator(self, split):
        if self.shouldCache:
            for i in self.ctx.cacheTracker.getOrCompute(self, split):
                yield i
        else:
            for i in self.compute(split):
                yield i

    ## Transformations (return a new RDD)

    def map(self, f):
        return MappedRDD(self, f)

    def filter(self, f):
        return FilteredRDD(self, f)

    def flatMap(self, f):
        return FlatMappedRDD(self, f)

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numSplits=None):
        aggregator = Aggregator()
        aggregator.createCombiner = createCombiner
        aggregator.mergeValue = mergeValue
        aggregator.mergeCombiners = mergeCombiners
        partitioner = HashPartitioner(numSplits)
        return ShuffledRDD(self, aggregator, partitioner)

    def reduceByKey(self, func, numSplits=None):
        return self.combineByKey(lambda x: x, func, func, numSplits)

    def groupByKey(self, numSplits=None):
        createCombiner = lambda x: [x]
        mergeValue = lambda c, v: c + [v]
        mergeCombiners = lambda c1, c2: c1 + c2
        return self.combineByKey(createCombiner, mergeValue, mergeCombiners, numSplits)

    # action (get result)

    def reduce(self, f):
        def reducePartition(it):
            if it:
                return [reduce(f, it)]
            else:
                return []
        options = self.ctx.runJob(self, reducePartition)
        return reduce(f, sum(options, []))

    def count(self):
        def ilen(x):
            return sum(1 for _ in x)
        return sum(self.ctx.runJob(self, lambda x: ilen(x)), 0)
    def collect(self):
        return sum(self.ctx.runJob(self, lambda x: list(x)), [])

    def saveAsTextFile(self, path):
        return OutputTextFileRDD(self, path).collect()


class MappedRDD(RDD):
    def __init__(self, prev, f=lambda x:x):
        RDD.__init__(self, prev.ctx)
        self.prev = prev
        self.f = f
        self.dependencies = [OneToOneDependency(prev)]

    @property
    def splits(self):
        return self.prev.splits

    def compute(self, split):
        return map(self.f, self.prev.iterator(split))

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['f']
        return d, dump_func(self.f)

    def __setstate__(self, state):
        self.__dict__, code = state
        self.f = load_func(code, globals())

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.prev)


class FlatMappedRDD(MappedRDD):
    def compute(self, split):
        for i in self.prev.iterator(split):
            for j in self.f(i):
                yield j


class FilteredRDD(MappedRDD):
    def compute(self, split):
        return filter(self.f, self.prev.iterator(split))


class ParallelCollectionSplit:
    def __init__(self, idx,  values):
        self.index = idx
        self.values = values


class ParallelCollection(RDD):
    def __init__(self, ctx, seq, numSlices):
        RDD.__init__(self, ctx)
        self.seq = seq
        self.numSlices = numSlices
        slices = self.slice(seq, numSlices)
        self._splits = [ParallelCollectionSplit(i, slices[i])
                        for i in range(len(slices))]

    def __repr__(self):
        return '<ParallelCollection(%d)>' % self.numSlices

    @property
    def splits(self):
        return self._splits

    def compute(self, split):
        return split.values

    def preferredLocations(self, split):
        return []

    @classmethod
    def slice(cls, seq, numSlices):
        m = len(seq)
        n = m / numSlices
        if m % numSlices != 0:
            n += 1
        seq = list(seq)
        return [seq[i*n: i*n+n] for i in range(numSlices)]


class ShuffledRDDSplit(Split):
    def __hash__(self):
        return self.index


class ShuffledRDD(RDD):
    def __init__(self, parent, aggregator, part):
        RDD.__init__(self, parent.ctx)
        self.parent = parent
        self.aggregator = aggregator
        self.partitioner = part
        self._splits = [ShuffledRDDSplit(i) for i in range(part.numPartitions)]
        self.dependencies = [ShuffleDependency(self.ctx.newShuffleId(), parent, aggregator, part)]

    def compute(self, split):
        combiners = {}
        def mergePair(k, c):
            combiners[k] = self.aggregator.mergeCombiners(combiners[k], c) if k in combiners else c
        fetcher = self.ctx.env.shuffleFetcher
        fetcher.fetch(self.dependencies[0].shuffleId, split.index, mergePair)
        return combiners.iteritems()

    def __repr__(self):
        return '<ShuffledRDD(%d) %s>' % (self.partitioner.numPartitions, self.parent)


class UnionSplit:
    def __init__(self, idx, rdd, split):
        self.index = idx
        self.rdd = rdd
        self.split = split


class UnionRDD(RDD):
    def __init__(self, ctx, rdds):
        RDD.__init__(self, ctx)
        self.rdds = rdds
        self._splits = []
        for rdd in rdds:
            for split in rdd.splits:
                self._splits.append(UnionSplit(len(self._splits), rdd, split))

    def compute(self, split):
        return split.rdd.iterator(split.split)


class TextFileRDD(RDD):
    def __init__(self, ctx, path, numSplits=None, splitSize=None):
        RDD.__init__(self, ctx)
        self.path = path
        if not os.path.exists(path):
            raise IOError('%s not exits' % path)
        size = os.path.getsize(path)
        if splitSize is None:
            if numSplits is None:
                splitSize = 64 * 1024 * 1024
            else:
                splitSize = size / numSplits
        n = size / splitSize
        if size % splitSize > 0:
            n += 1
        self.splitSize = splitSize
        self._splits = [Split(i) for i in range(n)]

    def compute(self, split):
        with open(self.path) as f:
            start = split.index * self.splitSize
            end = start + self.splitSize
            if start > 0:
                f.seek(start - 1)
                byte = f.read(1)
                skip = byte != '\n'
            else:
                f.seek(start)
                skip = False
            for line in f:
                if start >= end:
                    break
                start += len(line)
                if skip:
                    skip = False
                else:
                    yield line


class OutputTextFileRDD(RDD):
    def __init__(self, rdd, path):
        RDD.__init__(self, rdd.ctx)
        self.rdd = rdd
        self.path = path
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise Exception("Output path must be dir: %s" % path)
        else:
            os.makedirs(path)
        self.dependencies = [OneToOneDependency(rdd)]

    @property
    def splits(self):
        return self.rdd.splits

    def compute(self, split):
        path = os.path.join(self.path, str(split.index))
        with open(path, 'w') as f:
            for line in self.rdd.iterator(split):
                f.write(line)
                if not line.endswith('\n'):
                    f.write('\n')
        yield path
