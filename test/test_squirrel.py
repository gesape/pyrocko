
import unittest
import common

from pyrocko import squirrel, util


class SquirrelTestCase(unittest.TestCase):

    def test_detect(self):
        for (fn, format) in [
                ('test1.mseed', 'mseed'),
                ('test2.mseed', 'mseed'),
                ('test1.sac', 'sac'),
                ('test1.stationxml', 'stationxml'),
                ('test2.stationxml', 'stationxml'),
                ('test1.stations', 'pyrocko_stations'),
                ('test1.cube', 'datacube')]:

            fpath = common.test_data_file(fn)
            self.assertEqual(format, squirrel.detect_format(fpath))

    def test_add_nuts(self):
        sq = squirrel.Squirrel()
        for fn in ['test1.mseed', 'test2.mseed']:
            fpath = common.test_data_file(fn)
            sq.add_nuts(fpath)


if __name__ == "__main__":
    util.setup_logging('test_catalog', 'debug')
    unittest.main()
