from context import *
from rdd import *

import logging
logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG
)

d = '1 2 3 4'.split()
mc = MDparkContext('process')
nums = mc.makeRDD(d, 2)
print nums.map(lambda x: int(x)).map(lambda x: x + 1).reduce(lambda x, y: x + y)
