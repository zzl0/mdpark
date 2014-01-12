# coding: utf-8

class Env:
    def create(self, isMaster):
        from tracker import MapOutputTracker
        from fetch import SimpleShuffleFetcher
        self.mapOutputTracker = MapOutputTracker(isMaster)
        self.shuffleFetcher = SimpleShuffleFetcher()


env = Env()
