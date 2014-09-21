# coding: utf-8

class MapOutputTracker:
    def __init__(self, isMaster):
        self.serverUris = {}
        if isMaster:
            pass  # TODO

    def registerMapOutput(self, shuffleId, numMaps, mapId, serverUri):
        self.serverUris.setdefault(shuffleId, [None] * numMaps)[mapId] = serverUri

    def registerMapOutputs(self, shuffleId, locs):
        self.serverUris[shuffleId] = locs

    def unregisterMapOutput(self, shuffleId, mapId, serverUri):
        locs = self.serverUris.get(shuffleId, None)
        if locs is not None:
            if locs[mapId] == serverUri:
                locs[mapId] = None
    
    def getServerUris(self, shuffleId):
        return self.serverUris.get(shuffleId)

    def stop(self):
        self.serverUris.clear()
