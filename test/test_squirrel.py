
import os
import time
import unittest
import tempfile
import shutil

import common
from pyrocko import squirrel, util, pile


class SquirrelTestCase(unittest.TestCase):

    test_files = [
        ('test1.mseed', 'mseed'),
        ('test2.mseed', 'mseed'),
        ('test1.sac', 'sac'),
        ('test1.stationxml', 'stationxml'),
        ('test2.stationxml', 'stationxml'),
        ('test1.stations', 'pyrocko_stations'),
        ('test1.cube', 'datacube')]

    def test_detect(self):
        for (fn, format) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            self.assertEqual(format, squirrel.detect_format(fpath))

    def test_load(self):
        ii = 0
        for (fn, format) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            for nut in squirrel.iload(fpath, content=[]):
                ii += 1
        print ii

        assert ii == 396

        t1 = time.time()
        sq = squirrel.Squirrel()
        for (fn, _) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            for nut in squirrel.iload(fpath, content=[], squirrel=sq):
                ii += 1

        t2 = time.time()
        for (fn, _) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            for nut in squirrel.iload(fpath, content=[], squirrel=sq):
                ii += 1

        t3 = time.time()

        print t3 - t2, t2 - t1

    def benchmark_load(self):
        dir = '/tmp/testdataset_d'
        if not os.path.exists(dir):
            common.make_dataset(dir, tinc=36., tlen=1*common.D)

        if False:
            cachedirname = tempfile.mkdtemp('testcache')

            t1 = time.time()
            p = pile.make_pile(dir, fileformat='detect',
                               cachedirname=cachedirname)

            t2 = time.time()
            p = pile.make_pile(dir, fileformat='detect',
                               cachedirname=cachedirname)


            t3 = time.time()
            print 'pile', t2 - t1, t3 - t2

            shutil.rmtree(cachedirname)

        fns = sorted(util.select_files([dir]))

        t4 = time.time()
        ii = 0
        sq = squirrel.Squirrel('/tmp/squirrel.db')

        for fn in fns:
            for nut in squirrel.iload(fn, content=[], squirrel=sq, commit=False):
                ii += 1

        sq.commit()

        t5 = time.time()
        ii = 0
        for fn in fns:
            for nut in squirrel.iload(fn, content=[], squirrel=sq, commit=False):
                ii += 1

        sq.commit()

        t6 = time.time()

        ii = 0
        for fn in fns:
            for nut in squirrel.iload(fn, content=[], squirrel=sq,
                                      check_mtime=False, commit=False):
                ii += 1

        sq.commit()

        t6b = time.time()

        print 'xx'
        ii = 0
        for nut in squirrel.iload(fns, content=[], squirrel=sq, check_mtime=False):
            ii += 1

        t7 = time.time()

        print 'squirrel', t5 - t4, t6 - t5, t6b - t6, t7 - t6b



        ii = 0
        for x in sq.undig_raw():
            ii += 1

        t8 = time.time()


        ii = 0
        for fn, nuts in sq.undig_many(fns + ['xxx', 'yyy']):
            #if isinstance(x, squirrel.Nut):
            #    print x.file_name
            #else:
            #    print x

            ii += 1

        print ii

        t9 = time.time()

        print t8 - t7, t9 - t8





if __name__ == "__main__":
    util.setup_logging('test_catalog', 'warning')
    unittest.main()
