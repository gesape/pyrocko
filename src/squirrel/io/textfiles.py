from pyrocko.squirrel import model
import logging

logger = logging.getLogger('pyrocko.squirrel.io_backends.textfiles')


def provided_formats():
    return ['pyrocko_stations']


def detect_pyrocko_stations(first512):
    for line in first512.splitlines():
        t = line.split(None, 5)
        if len(t) in (5, 6):
            if len(t[0].split('.')) != 3:
                return False

            try:
                lat, lon, ele, dep = map(float, t[1:5])
                if lat < -90. or 90 < lat:
                    return False
                if lon < -180. or 180 < lon:
                    return False

                return True

            except:
                raise
                return False

    return False


def detect(first512):
    if detect_pyrocko_stations(first512):
        return 'pyrocko_stations'

    return None


def float_or_none(s):
    if s.lower() == 'nan':
        return None
    else:
        return float(s)


def iload(format, filename, segment, mtime, content):
    source = dict(
        file_name=filename,
        file_format=format,
        file_segment=0,
        file_mtime=mtime)

    inut = 0
    tmin = None
    tmax = None
    with open(filename, 'r') as f:

        have_station = False
        for (iline, line) in enumerate(f):
            try:
                toks = line.split(None, 5)
                if len(toks) == 5 or len(toks) == 6:
                    net, sta, loc = toks[0].split('.')
                    lat, lon, elevation, depth = [float(x) for x in toks[1:5]]
                    if len(toks) == 5:
                        description = u''
                    else:
                        description = unicode(toks[5])

                    agn = ('', 'FDSN')[net != '']

                    nut = model.make_station_nut(
                        file_element=inut,
                        agency=agn,
                        network=net,
                        station=sta,
                        location=loc,
                        tmin=tmin,
                        tmax=tmax,
                        **source)

                    if 'station' in content:
                        nut.content = model.Station(
                            lat=lat,
                            lon=lon,
                            elevation=elevation,
                            depth=depth,
                            description=description,
                            **nut.station_kwargs)

                    yield nut
                    inut += 1

                    have_station = True

                elif len(toks) == 4 and have_station:
                    cha = toks[0]
                    azi = float_or_none(toks[1])
                    dip = float_or_none(toks[2])
                    gain = float(toks[3])

                    if gain != 1.0:
                        logger.warn('%s.%s.%s.%s gain value from stations '
                                    'file ignored - please check' % (
                                        net, sta, loc, cha))

                    nut = model.make_channel_nut(
                        file_element=inut,
                        agency=agn,
                        network=net,
                        station=sta,
                        location=loc,
                        channel=cha,
                        tmin=tmin,
                        tmax=tmax,
                        **source)

                    if 'channel' in content:
                        nut.content = model.Channel(
                            lat=lat,
                            lon=lon,
                            elevation=elevation,
                            depth=depth,
                            azimuth=azi,
                            dip=dip,
                            **nut.channel_kwargs)

                    yield nut
                    inut += 1

                else:
                    raise Exception('invalid syntax')

            except Exception, e:
                logger.warn('skipping invalid station/channel definition: %s '
                            '(line: %i, file: %s' % (str(e), iline, filename))
