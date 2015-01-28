import numpy as num
import sqlite3


def tsplit(t):
    seconds = int(num.floor(t))
    offset = float(t - seconds)


class Nut(Object):
    kind = String.T()

    agency = String.T(default='FDSN', optional=True, help='Agency code (2-5)')
    network = String.T(help='Deployment/network code (1-8)')
    station = String.T(help='Station code (1-5)')
    location = String.T(optional=True, help='Location code (0-2)')
    channel = String.T(optional=True, help='Channel code (3)')
    extra = String.T(optional=True, help='Extra/custom code')

    tmin_seconds = Timestamp.T(optional=True)
    tmin_offset = Float.T(default=0.0)
    tmax_seconds = Timestamp.T(optional=True)
    tmax_offset = Float.T(default=0.0)

    deltat = Float.T(optional=True)

    file_name = String.T(optional=True)
    file_segment = Int.T(optional=True)
    file_element = Int.T(optional=True)
    file_format = String.T(optional=True)

    file_mtime = Timestamp.T(optional=True)

    sql_create_table = '''CREATE TABLE nuts (
            kind text,
            agency text,
            network text,
            station text,
            location text,
            channel text,
            extra text,
            tmin_seconds integer,
            tmin_offset float,
            tmax_seconds integer,
            tmax_offset float,
            deltat float,
            file_mtime float,
            file_name text,
            file_segment int,
            file_element int,
            file_format text)'''

    sql_insert = 'INSERT INTO nuts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

    def __init__(self, **kwargs):
        for k in ('tmin', 'tmax'):
            if k in kwargs:
                seconds, offset = tsplit(kwargs.pop(k))
                kwargs[k + '_seconds'] = seconds
                kwargs[k + '_offset'] = offset

        Object.__init__(self, **kwargs)

    def values(self):
        return (
            self.kind,self.agency, self.network, self.station, self.location,
            self.channel, self.extra, self.tmin_seconds, self.tmin_offset,
            self.tmax_seconds, self.tmax_offset, self.deltat,
            self.file_mtime, self.file_name, self.file_segment,
            self.file_element, self.file_format)


class Waveform(Object):
    pass


class Station(Object):
    pass


class Channel(Object):
    pass


class Response(Object):
    pass


class Event(Object):
    pass


class Squirrel(object):
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        c = self.conn.cursor()
        c.execute(Nut.sql_create_table)
        c.commit()

    def add_nut(self, nut):
        c = self.conn.cursor()
        c.execute(Nut.sql_insert, nut.values())

    def add_nuts(self, filename):
        for nut, _ in io.iload(filename, format='detect'):
            self.add_nut(nut)

    def waveform(self, selection=None, **kwargs):
        pass

    def waveforms(self, selection=None, **kwargs):
        pass

    def station(self, selection=None, **kwargs):
        pass

    def stations(self, selection=None, **kwargs):
        pass

    def channel(self, selection=None, **kwargs):
        pass

    def channels(self, selection=None, **kwargs):
        pass

    def response(self, selection=None, **kwargs):
        pass

    def responses(self, selection=None, **kwargs):
        pass

    def event(self, selection=None, **kwargs):
        pass

    def events(self, selection=None, **kwargs):
        pass


sq = Squirrel()
sq.add('/path/to/data')
station = sq.add(Station(...))
waveform = sq.add(Waveform(...))

sq.remove(station)

stations = sq.stations()
for waveform in sq.waveforms(stations):
    resp = sq.response(waveform)
    resps = sq.responses(waveform)
    station = sq.station(waveform)
    channel = sq.channel(waveform)
    station = sq.station(channel)
    channels = sq.channels(station)
    responses = sq.responses(channel)
    lat, lon = sq.latlon(waveform)
    lat, lon = sq.latlon(station)
    dist = sq.distance(station, waveform)
    azi = sq.azimuth(channel, station)

