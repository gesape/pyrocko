import unittest
import os
import time
from pyrocko import util
import functools
import logging
import socket

logger = logging.getLogger('test.common')

benchmark_results = []


def test_data_file_no_download(fn):
    return os.path.join(os.path.split(__file__)[0], 'data', fn)


def test_data_file(fn):
    fpath = test_data_file_no_download(fn)
    if not os.path.exists(fpath):
        if not have_internet():
            raise unittest.SkipTest(
                'need internet access to download data file')

        url = 'http://kinherd.org/pyrocko_test_data/' + fn
        logger.info('downloading %s' % url)
        util.download_file(url, fpath)

    return fpath


def have_internet():
    try:
        return 0 < len([
            (s.connect(('8.8.8.8', 80)), s.close())
            for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]])

    except OSError:
        return False


require_internet = unittest.skipUnless(have_internet(), 'need internet access')


class Benchmark(object):
    def __init__(self, prefix=None):
        self.prefix = prefix or ''
        self.show_factor = False
        self.results = []

    def __call__(self, func):
        def stopwatch(*args):
            t0 = time.time()
            name = self.prefix + func.__name__
            result = func(*args)
            elapsed = time.time() - t0
            self.results.append((name, elapsed))
            return result
        return stopwatch

    def labeled(self, label):
        def wrapper(func):
            @functools.wraps(func)
            def stopwatch(*args):
                t0 = time.time()
                result = func(*args)
                elapsed = time.time() - t0
                self.results.append((label, elapsed))
                return result
            return stopwatch
        return wrapper

    def __str__(self, header=True):
        if not self.results:
            return 'No benchmarks ran'
        tmax = max([r[1] for r in self.results])

        rstr = ['Benchmark results']
        if self.prefix != '':
            rstr[-1] += ' - %s' % self.prefix

        if self.results:
            indent = max([len(name) for name, _ in self.results])
        else:
            indent = 0
        rstr.append('=' * (indent + 17))
        rstr.insert(0, rstr[-1])

        if not header:
            rstr = []

        for res in self.results:
            rstr.append(
                '{0:<{indent}}{1:.8f} s'.format(*res, indent=indent+5))
            if self.show_factor:
                rstr[-1] += '{0:8.2f} x'.format(tmax/res[1])
        if len(self.results) == 0:
            rstr.append('None ran!')

        return '\n'.join(rstr)

    def clear(self):
        self.results = []
