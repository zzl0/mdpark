# coding: utf-8

import urllib
import logging
import pickle

from mdpark.env import env


class ShuffleFetcher:
    def fetch(self, shuflleId, reduceId, func):
        raise NotImplementedError

    def stop(self):
        pass


class SimpleShuffleFetcher(ShuffleFetcher):
    def fetch(self, shuflleId, reduceId, func):
        logging.info('Fetching outputs for shuflle %d, reduce %d',
                shuflleId, reduceId)
        uri2splits = {}
        serverUris = env.mapOutputTracker.getServerUris(shuflleId)

        for i, uri in enumerate(serverUris):
            uri2splits.setdefault(uri, []).append(i)

        for uri, ids in uri2splits.items():
            for id in ids:
                try:
                    url = "%s/%d/%d/%d" % (uri, shuflleId, id, reduceId)
                    logging.info('fetch %s', url)
                    for k, v in pickle.loads(urllib.urlopen(url).read()):
                        logging.info('read %s: %s', k, v)
                        func(k, v)
                except IOError:
                    logging.error("fetch failed for %s", url)
                    raise
