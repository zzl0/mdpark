# coding: utf-8

import logging
import Queue

from dependency import *
from accumulator import *
from task import *

class TaskEndReason: pass
class Success(TaskEndReason): pass
class FetchFailed: pass

logger = logging.getLogger("scheduler")

class OtherFailure:
    def __init__(self, message):
        self.message = message


POLL_TIMEOUT = 1
RESUBMIT_TIMEOUT = 60


class Stage:
    """
    A stage is a set of independent tasks all computing the same function that
    need to run as part of a Spark job, where all the tasks have the same shuffle
    dependencies. Each DAG of tasks run by the scheduler is split up into stages
    at the boundaries where shuffle occurs, and then the DAGScheduler runs these
    stages in topological order.

    Each Stage can either be a shuffle map stage, in which case its tasks' results
    are input for anthoer stage, or a result stage, in which case its tasks directly
    compute the action that initiated a job (e.g. collect(), etc). For shuffle
    map stages, we also track the nodes that each output partition is on.
    """
    def __init__(self, id, rdd, shuffleDep, parents):
        self.id = id
        self.rdd = rdd
        self.shuffleDep = shuffleDep  # Output shuffle if stage is a map stage
        self.parents = parents
        self.isShuffleMap = shuffleDep != None
        self.numPartitions = len(rdd.splits)
        self.outputLocs = [[]] * self.numPartitions
        self.numAvailableOutputs = 0

    def __str__(self):
        return '<Stage(%d) for %s>' % (self.id, self.rdd)

    def __repr__(self):
        return str(self)

    @property
    def isAvaiable(self):
        if not self.parents and not self.isShuffleMap:
            return True
        return self.numAvailableOutputs == self.numPartitions

    def addOutputLoc(self, partition, host):
        prevList = self.outputLocs[partition]
        self.outputLocs[partitions] = [host] + prevList
        if not prevList:
            self.numAvailableOutputs += 1

    def removeOutput(self, partition, host):
        prev = self.outputLocs[partition]
        self.outputLocs[partition] = [h for h in prev if h != host]
        if prev and not self.outputLocs[partition]:
            self.numAvailableOutputs -= 1

    def __hash__(self):
        return self.id


class Scheduler:
    def start(self): pass
    def waitForRegister(self): pass
    def runJob(self, rdd, func, partitions, allowLocal): pass
    def stop(self): pass
    def defaultParallelism(self): pass


class CompletionEvent:
    def __init__(self, task, reason, result, accumUpdates):
        self.task = task
        self.reason = reason
        self.result = result
        self.accumUpdates = accumUpdates


class DAGScheduler(Scheduler):
    """
    A Scheduler subclass that implements stage-oriented scheduling.
    It computes a DAG of stages for each job, keep track of which RDDs
    and stage outputs are materialized, and computes a minimal schedule
    to run the job. Subclasses only need to implement the code to send
    a task to the cluster and to report fetch failures (the submitTasks
    method, and code to add CompletionEvents).
    """
    nextStageId = 0

    def __init__(self):
        self.completionEvents = Queue.Queue()
        self.idToStage = {}
        self.shuffleToMapStage = {}

        self.waiting = set()  # Stages we need to run whose parents aren't done
        self.running = set()  # Stages we are running right now.
        self.failed = set()   # Stages that must be resubmitted due to fetch failures
        self.pendingTasks = {}  # Missing tasks from each stage
        self.lastFetchFailureTime = 0  # Used to wait a bit to avoid repeated resubmits

        self.finished = set()  # finished partition id
        self.outputParts = []
        self.numOutputParts = 0
        self.func = None

    def submitTasks(self, tasks):
        pass

    def taskEnded(self, task, reason, result, accumUpdates):
        self.completionEvents.put(CompletionEvent(task, reason, result, accumUpdates))

    def newStageId(self):
        self.nextStageId += 1
        return self.nextStageId

    def newStage(self, rdd, shuffleDep):
        """
        Create a Stage for the given RDD, either as a shuffle map stage (for a
        ShuffleDependency) or as a result stage for the final RDD used directly
        in an action.
        """
        id = self.newStageId()
        stage = Stage(id, rdd, shuffleDep, self.getParentStages(rdd))
        self.idToStage[id] = stage
        logger.debug("new stage: %s", stage)
        return stage
        
    def getParentStages(self, rdd):
        """
        Get or create the list of parent stages for a given RDD.
        """
        parents = set()
        visited = set()
        def visit(r):
            if r.id in visited:
                return
            visited.add(r.id)
            for dep in r.dependencies:
                if isinstance(dep, ShuffleDependency):
                    parents.add(self.getShuffleMapStage(dep))
                else:
                    visit(dep.rdd)
        visit(rdd)
        return list(parents)

    def getShuffleMapStage(self, shuffleDep):
        stage = self.shuffleToMapStage.get(shuffleDep.shuffleId, None)
        if stage is None:
            stage = self.newStage(shuffleDep.rdd, shuffleDep)
            self.shuffleToMapStage[shuffleDep.shuffleId] = stage
        return stage

    def getMissingParentStages(self, stage):
        missing = set()
        visited = set()
        def visit(r):
            if r.id in visited:
                return
            visited.add(r.id)
            for i in range(len(r.splits)):
                for dep in r.dependencies:
                    if isinstance(dep, ShuffleDependency):
                        stage = self.getShuffleMapStage(dep)
                        if not stage.isAvailable:
                            missing.add(stage)
                    elif isinstance(dep, NarrowDependency):
                        visit(dep.rdd)
        visit(stage.rdd)
        return list(missing)

    def submitStage(self, stage):
        logger.debug("submitStage(%r)", stage)
        if stage not in self.waiting and stage not in self.running:
            missing = self.getMissingParentStages(stage)
            logger.debug("missing:%r", missing)
            if not missing:
                logger.info("Submitting %r, which has no missing parents", stage)
                self.submitMissingTasks(stage)
                self.running.add(stage)
            else:
                for p in self.missing:
                    self.submitStage(p)
                self.waiting.add(stage)

    def submitMissingTasks(self, stage):
        logger.debug("submitMissingTasks(%s)", stage)
        myPending = self.pendingTasks.setdefault(stage, set())
        tasks = []
        if not stage.isShuffleMap:
            for i in range(self.numOutputParts):
                if i not in self.finished:
                    part = self.outputParts[i]
                    locs = self.getPreferedLocs(stage.rdd, part)
                    tasks.append(ResultTask(stage.id, stage.rdd, self.func, part, locs, i))
        else:
            # This is shuffle map
            for i in range(stage.numPartitions):
                if stage.outputLocs[i] is None:
                    locs = self.getPreferedLocs(stage.rdd, i)
                    tasks.append(ShuffleMapTask(stage.id, stage.rdd, stage.shuffleDep.get, i, locs))
        logger.debug('add to pending %r', tasks)
        myPending |= set(t.id for t in tasks)
        self.submitTasks(tasks)


    def runJob(self, finalRdd, func, partitions, allowLocal):
        self.outputParts = list(partitions)
        self.numOutputParts = len(partitions)
        self.func = func
        finalStage = self.newStage(finalRdd, None)
        results = [None] * self.numOutputParts
        numFinished = 0

        logger.debug("Final stage: %s, %d", finalStage, self.numOutputParts)
        logger.debug("Parents of final stage: %s", finalStage.parents)
        logger.debug("Missing parents: %s", self.getMissingParentStages(finalStage))

        if allowLocal and not finalStage.parents and self.numOutputParts == 1:
            split = finalRdd.splits[self.outputParts[0]]
            taskContext = TaskContext(finalStage.id, self.outputParts[0], 0)
            return list(func(taskContext, finalRdd.iterator(split)))

        self.submitStage(finalStage)

        while numFinished != self.numOutputParts:
            evt = self.completionEvents.get(POLL_TIMEOUT)
            if evt:
                task = evt.task
                stage = self.idToStage[task.stageId]
                logger.debug('remove from pending %s %s', self.pendingTasks[stage], task)
                self.pendingTasks[stage].remove(task.id)
                if isinstance(evt.reason, Success):
                    Accumulator.add(evt.accumUpdates)
                    if isinstance(task, ResultTask):
                        results[task.outputId] = evt.result
                        self.finished.add(task.outputId)
                        numFinished += 1
                    elif isinstance(task, ShuffleMapTask):
                        stage = idToStage[task.stageId]
                        stage.addOutputLoc(task.partition, evt.result)
                        if not self.pendingTasks[stage]:
                            logger.debug('%s finished; looking for newly runnable stages', stage)
                            self.running.remove(stage)
                            newlyRunnable = set(stage for stage in waiting if self.getMissingParentStages(stage))
                            self.waiting -= newlyRunnable
                            self.running |= newlyRunnable
                            for stage in newlyRunnable:
                                self.submitMissingTasks(stage)
                else:
                    raise

            if self.failed and time.time() > lastFetchFailureTime + RESUBMIT_TIMEOUT:
                logger.debug('resubmitting failed stages')
                for stage in self.failed:
                    self.submitStage(stage)
                self.failed.clear()
        return results
    
    def getPreferedLocs(self, rdd, partition):
        return rdd.preferredLocations(rdd.splits[partition])


def process_worker(task, aid):
    try:
        Accumulator.clear()
        logger.debug('run task %s %d', task, aid)
        result = task.run(aid)
        logger.debug('task %s result %s', task, result)
        accumUpdates = Accumulator.values()
        return (task, Success(), result, accumUpdates)
    except Exception, e:
        logger.debug('error in task %s, error: %s', task, e)
        import traceback
        traceback.print_exec()
        return (task, OtherFailure('exception:' + str(e)), None, None)


class MultiProcessScheduler(DAGScheduler):
    attemptId = 0

    def nextAttemptId(self):
        self.attemptId += 1
        return self.attemptId

    def __init__(self, num):
        DAGScheduler.__init__(self)
        from multiprocessing import Pool, Queue
        self.reply = Queue()
        self.pool = Pool(num)

    def submitTasks(self, tasks):
        def callback(args):
            self.taskEnded(*args)

        for task in tasks:
            aid = self.nextAttemptId()
            logger.debug('put task sync %s', task)
            self.pool.apply_async(process_worker, (task, aid), {}, callback)

    def stop(self):
        self.pool.close()
        self.pool.join()
