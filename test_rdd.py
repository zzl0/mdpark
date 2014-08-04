import unittest
from context import *
from rdd import *

import logging
logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG
)


class TestRDD(unittest.TestCase):
    def setUp(self):
        self.mc = MDparkContext('local')

    def tearDown(self):
        self.mc.stop()

    def print_sep(self, name):
        print '================ %s starts =============' % name

    def test_reduceByKey(self):
        self.print_sep('test_reduceByKey')

        d = zip([1,2,3,3], range(4,8))
        nums = self.mc.makeRDD(d, 2)
        rs = nums.reduceByKey(lambda x, y: x + y, 2).collect()
        expected = [(2, 5), (1, 4), (3, 13)]
        self.assertEqual(rs, expected)

    def test_file(self):
        self.print_sep('test_file')

        rdd = self.mc.makeRDD(range(20), 5)
        rdd = rdd.map(lambda x: str(x))

        tmp_path = '/tmp/test_file'
        rdd.saveAsTextFile(tmp_path)

        rdd = self.mc.textFile(tmp_path)
        rdd = rdd.map(lambda x: int(x))
        self.assertEqual(sum(rdd.collect()), sum(range(20)))


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.ERROR)
    unittest.main()
