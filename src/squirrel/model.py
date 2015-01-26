class Nut(Object):
    codes = Tuple.T(
        6, String.T(),
        default=('', '', '', '', '', ''),
        help='codes according to IASPEI station coding standard '
             'plus a custom field to e.ge'
             '(Agency (2-5), Deployment (1-8), Station (1-5), Location (0-2), '
             'Channel (3), Custom)')

    tzero = Timestamp.T(default=0.0)
    otmin = Float.T(optional=True)
    otmax = Float.T(optional=True)
    deltat = Float.T(optional=True)
    mtime = Timestamp.T(optional=True)
    location = String.T(optional=True)
    segment = String.T(optional=True)
    format = String.T(optional=True)


class Waveform(Object):
    pass


class Station(Object):
    pass


class Channel(Object):
    pass


class Response(Object):
    pass
