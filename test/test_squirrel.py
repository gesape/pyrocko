
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

    def test_dig_undig(self):
        nuts = []
        for file_name in 'abcde':
            for file_element in xrange(2):
                nuts.append(squirrel.Nut(
                    file_name=file_name,
                    file_format='test',
                    file_mtime=0.0,
                    file_segment=0,
                    file_element=file_element,
                    kind='test'))

        sq = squirrel.Squirrel()
        sq.dig(nuts)

        for file_name in 'abcde':
            nuts2 = sq.undig(file_name)
            print len(nuts2)
            for nut in nuts2:
                print nut.file_name, nut.file_element

        for fn, nuts2 in sq.undig_many(filenames=['a', 'b']):
            print fn
            for nut in nuts2:
                print nut.file_name, nut.file_element
        


    def benchmark_load(self):
        dir = '/tmp/testdataset_d'
        if not os.path.exists(dir):
            common.make_dataset(dir, tinc=36., tlen=1*common.D)

        fns = sorted(util.select_files([dir]))

        if False:
            cachedirname = tempfile.mkdtemp('testcache')

            t1 = time.time()
            p = pile.make_pile(fns, fileformat='detect',
                               cachedirname=cachedirname)


            t2 = time.time()
            print 'pile, initial scan: %g' % (t2 - t1)

            p = pile.make_pile(fns, fileformat='detect',
                               cachedirname=cachedirname)


            t3 = time.time()
            print 'pile, rescan: %g' % (t3 - t2)

            shutil.rmtree(cachedirname)


        t4 = time.time()
        ii = 0
        dbfilename = '/tmp/squirrel.db'
        if os.path.exists(dbfilename):
            os.unlink(dbfilename)
        sq = squirrel.Squirrel(dbfilename)

        for nut in squirrel.iload(fns, content=[], squirrel=sq):
            ii += 1

        t5 = time.time()

        print 'squirrel, initial scan: %g' % (t5 - t4)

        ii = 0
        for nut in squirrel.iload(fns, content=[], squirrel=sq):
            ii += 1

        t6 = time.time()

        print 'squirrel, rescan: %g' % (t6 - t5)

        ii = 0
        for nut in squirrel.iload(fns, content=[], squirrel=sq,
                                  check_mtime=False):
            ii += 1

        t7 = time.time()
        print 'squirrel, rescan, no mtime check: %g' % (t7 - t6)

        for fn, nuts in sq.undig_many(fns):
            ii += 1

        t8 = time.time()

        print 'squirrel, pure undig: %g' % (t8 - t7)

        sq.choose(fns)

        t9 = time.time()

        print 'squirrel, select workload: %g' % (t9 - t8)


if __name__ == "__main__":
    util.setup_logging('test_catalog', 'warning')
    unittest.main()
