from context import *
from rdd import *

import logging
logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG
)

d = '1 2 3 4 2 3'.split()
mc = MDparkContext('local')
nums = mc.makeRDD(d, 2)
#print nums.map(lambda x: int(x)).map(lambda x: x + 1).reduce(lambda x, y: x + y)
#print nums.map(lambda x: (x, int(x))).reduceByKey(lambda x, y: x + y, 2).collect()

def test_groupByKey():
    d = zip([1,2,3,3], range(4,8))
    nums = mc.makeRDD(d, 2)
    print nums.groupByKey(2).collect()


def test_reduceByKey():
    d = zip([1,2,3,3], range(4,8))
    nums = mc.makeRDD(d, 2)
    print nums.reduceByKey(lambda x, y: x + y, 2).collect()


def main():
    test_groupByKey()
    test_reduceByKey()
    print 'tests passed'


if __name__ == '__main__':
    main()
