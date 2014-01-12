# coding: utf-8

class Dependency:
    def __init__(self, rdd):
        self.rdd = rdd


class NarrowDependency(Dependency):
    isShuffle = False

    def getParents(self, outputPartition):
        pass


class OneToOneDependency(NarrowDependency):
    def getParents(self, partitionId):
        return [partitionId]


class ShuffleDependency(Dependency):
    isShuffle = True

    def __init__(self, shuffleId, rdd, aggregator, partitioner):
        Dependency.__init__(self, rdd)
        self.shuffleId = shuffleId
        self.aggregator = aggregator
        self.partitioner = partitioner


class Aggregator:
    def createCombiner(self, v): pass
    def mergeValue(self, c, v): pass
    def mergeCombiners(self, c, v): pass


class Partitioner:
    @property
    def numPartitions(self):
        pass

    def getPartition(self, key):
        pass


class HashPartitioner(Partitioner):
    def __init__(self, partitions):
        self.partitions = partitions

    @property
    def numPartitions(self):
        return self.partitions

    def getPartition(self, key):
        return hash(key) % self.partitions

    def __equal__(self, other):
        return other.numPartitions == self.numPartitions
