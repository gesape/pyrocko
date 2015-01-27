class Nut(Object):
    codes = Tuple.T(
        6, String.T(),
        default=('', '', '', '', '', ''),
        help='codes according to IASPEI station coding standard '
             'plus a custom field '
             '(Agency (2-5), Deployment/Network (1-8), Station (1-5), '
             'Location (0-2), Channel (3), Custom)')

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


class Squirrel(object):

    def add(self, thing):
        pass

    def waveforms(self, selection):
        pass

    def stations(self, selection):
        pass

    def channels(self, selection)
        pass

    def responses(self, selection):
        pass
    

s = Squirrel()
s.add('data')
s.add_station(station)
stations = s.stations()
for waveforms in s.chopper(stations, tinc=10.):
    for waveform in waveforms:
        resp = s.response(waveform)
        resps = s.responses(waveform)
        station = s.station(waveform)
        channel = s.channel(waveform)
        station = s.station(channel)
        channels = s.channels(station)
        responses = s.responses(channel)
        lat, lon = s.latlon(waveform)
        lat, lon = s.latlon(station)
        dist = s.distance(station, waveform)
        azi = s.azimuth(channel, station)

        
