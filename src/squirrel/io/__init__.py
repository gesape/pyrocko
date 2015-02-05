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


def iload(filename, segment=None, format='detect', squirrel=None,
          check_mtime=True, commit=True,
          content=['waveform', 'station', 'channel', 'response', 'event']):

    mtime = None
    if squirrel:
        if check_mtime:
            mtime = get_mtime(filename)

        old_nuts = squirrel.undig(filename, segment, mtime)
        if old_nuts:
            db_only_operation = not content or all(
                nut.kind in content and nut.content_in_db for nut in old_nuts)

            if db_only_operation:
                logger.debug('using cached information for file %s, '
                             'segment %s' % (filename, segment))

                for nut in old_nuts:
                    if nut.kind in content:
                        squirrel.undig_content(nut)

                    yield nut

                return
    else:
        old_nuts = None

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

    nuts = []
    logger.debug('reading file %s, segment %s' % (filename, segment))
    for nut in mod.iload(format, filename, segment, mtime, content):
        nuts.append(nut)
        yield nut

    if squirrel:
        squirrel.dig(nuts)
        if commit:
            squirrel.commit()


def iload_many(
        filenames, format='detect', squirrel=None, check_mtime=True, commit=True,
        content=['waveform', 'station', 'channel', 'response', 'event']):

    if squirrel:
        it = squirrel.undig_many(filenames)
    else:
        it = ((fn, []) for fn in filenames)

    for filename, old_nuts in it:
        mtime = None
        
        if old_nuts:
            if check_mtime:
                mtime = get_mtime(filename)
                mtime_latest = mtime
            else:
                if old_nuts:
                    mtime_latest = max(nut.file_mtime for nut in old_nuts)

            old_nuts_uptodate = [nut for nut in old_nuts if nut.file_mtime == mtime_latest]

            if len(old_nuts) != len(old_nuts_uptodate):
                print 'del'
                squirrel.delete_outdated(filename, mtime)

            if old_nuts:
                db_only_operation = not content or all(
                    nut.kind in content and nut.content_in_db for nut in old_nuts)

                if db_only_operation:
                    logger.debug('using cached information for file %s, '
                                 % filename)

                    for nut in old_nuts:
                        if nut.kind in content:
                            squirrel.undig_content(nut)

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

        nuts = []
        logger.debug('reading file %s' % filename)
        for nut in mod.iload(format, filename, None, mtime, content):
            nuts.append(nut)
            yield nut

        if squirrel:
            squirrel.dig(nuts)

    if commit:
        squirrel.commit()

        












