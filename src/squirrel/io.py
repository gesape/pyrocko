import os

from pyrocko.io_common import FileLoadError
from io_backends import mseed, sac, datacube, stationxml, textfiles

backend_modules = [mseed, sac, datacube, stationxml, textfiles]


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


def detect_format(filename, mtime=None):
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


def iload(filename, format='detect',
          want=['waveforms', 'stations', 'channels', 'responses']):

    mtime = get_mtime(filename)

    if format == 'detect':
        format = detect_format(filename, mtime=mtime)

    if format not in format_providers:
        raise UnknownFormat(format)

    mod = format_providers[format]

    return mod.iload(filename, format, mtime, want)
