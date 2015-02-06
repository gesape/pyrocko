import os
import logging

from pyrocko.io_common import FileLoadError
from pyrocko.squirrel.io import mseed, sac, datacube, stationxml, textfiles

backend_modules = [mseed, sac, datacube, stationxml, textfiles]


logger = logging.getLogger('pyrocko.sqirrel.io')


def update_format_providers():
    '''Update global mapping from file format to io backend module.'''

    global format_providers
    format_providers = {}
    for mod in backend_modules:
        for format in mod.provided_formats():
            if format not in format_providers:
                format_providers[format] = []

            format_providers[format].append(mod)

format_providers = {}
update_format_providers()


def detect_format(filename):
    '''Determine file type from first 512 bytes.'''

    try:
        with open(filename, 'r') as f:
            data = f.read(512)

    except OSError, e:
        raise FileLoadError(e)

    fmt = None
    for mod in backend_modules:
        fmt = mod.detect(data)
        if fmt is not None:
            return fmt

    raise FormatDetectionFailed(filename)


class FormatDetectionFailed(FileLoadError):
    def __init__(self, filename):
        FileLoadError.__init__(
            self, 'format detection failed for file: %s' % filename)


class UnknownFormat(FileLoadError):
    def __init__(self, format):
        FileLoadError.__init__(
            self, 'unknown format: %s' % format)


def get_mtime(filename):
    try:
        return os.stat(filename)[8]
    except OSError, e:
        raise FileLoadError(e)


def iload(
        filenames,
        segment=None,
        format='detect',
        squirrel=None,
        check_mtime=True,
        commit=True,
        content=['waveform', 'station', 'channel', 'response', 'event']):

    n_db = 0
    n_load = 0

    if isinstance(filenames, basestring):
        filenames = [filenames]
        few = True
    else:
        assert segment is None, \
            'segment argument can only be used when loading from a single file'

        try:
            few = len(filenames) < 100
        except TypeError:
            few = False

    if squirrel:
        if not few:
            it = squirrel.undig_many(filenames)
        else:
            it = ((fn, squirrel.undig(fn)) for fn in filenames)

    else:
        it = ((fn, []) for fn in filenames)

    for filename, old_nuts in it:
        mtime = None
        if check_mtime and old_nuts:
            mtime = get_mtime(filename)
            if mtime != old_nuts[0].file_mtime:
                old_nuts = []

        if segment is not None:
            old_nuts = [nut for nut in old_nuts if nut.segment == segment]

        if old_nuts:
            db_only_operation = not content or all(
                nut.kind in content and nut.content_in_db for nut in old_nuts)

            if db_only_operation:
                logger.debug('using cached information for file %s, '
                             % filename)

                for nut in old_nuts:
                    if nut.kind in content:
                        squirrel.undig_content(nut)

                    n_db += 1
                    yield nut

                continue

        if mtime is None:
            mtime = get_mtime(filename)

        if format == 'detect':
            if old_nuts and old_nuts[0].file_mtime == mtime:
                format = old_nuts[0].file_format
            else:
                format = detect_format(filename)

        if format not in format_providers:
            raise UnknownFormat(format)

        mod = format_providers[format][0]

        logger.debug('reading file %s' % filename)
        nuts = []
        for nut in mod.iload(format, filename, segment, content):
            nut.file_name = filename
            nut.file_format = format
            nut.file_mtime = mtime

            nuts.append(nut)
            n_load += 1
            yield nut

        if squirrel and nuts != old_nuts:
            if segment is not None:
                nuts = mod.iload(format, filename, None, [])
                for nut in nuts:
                    nut.file_name = filename
                    nut.file_format = format
                    nut.file_mtime = mtime

            squirrel.dig(nuts)

    if squirrel and commit:
        squirrel.commit()

    #print 'from db: %i, from files: %i' % (n_db, n_load)
