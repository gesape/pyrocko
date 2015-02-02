
import unittest
import common

from pyrocko import squirrel, util


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

        sq = squirrel.Squirrel()
        for (fn, format) in SquirrelTestCase.test_files:
            fpath = common.test_data_file(fn)
            for nut in squirrel.iload(fpath, content=[], squirrel=sq):
                ii += 1



    def test_add_nuts(self):
        sq = squirrel.Squirrel()
        for fn in ['test1.mseed', 'test2.mseed']:
            fpath = common.test_data_file(fn)
            sq.add_nuts(fpath)


if __name__ == "__main__":
    util.setup_logging('test_catalog', 'warning')
    unittest.main()
