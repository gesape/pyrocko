import os.path as op
import os
import tempfile

import numpy as num

from pyrocko import util, io, trace

H = 3600.
D = 3600.*24.


def make_dataset(dir=None, nstations=10, nchannels=3, tlen=10*D, deltat=0.01,
                 tinc=1*H):

    if dir is None:
        dir = tempfile.mkdtemp('_test_squirrel_dataset')

    tref = util.str_to_time('2015-01-01 00:00:00')

    nblocks = int(round(tlen / tinc))

    for istation in xrange(nstations):
        for ichannel in xrange(nchannels):
            for iblock in xrange(nblocks):
                tmin = tref + iblock*tinc
                nsamples = int(round(tinc/deltat))
                ydata = num.random.randint(-1000, 1001, nsamples).astype(
                    num.int32)

                tr = trace.Trace(
                    '', '%04i' % istation, '', '%03i' % ichannel,
                    tmin=tmin,
                    deltat=deltat,
                    ydata=ydata)

                io.save([tr], op.join(dir, '%s/%c/%b.mseed'))

    return dir


def test_data_file(fn):
    fpath = os.path.join(os.path.split(__file__)[0], 'data', fn)
    if not os.path.exists(fpath):
        url = 'http://kinherd.org/pyrocko_test_data/' + fn
        util.download_file(url, fpath)

    return fpath
