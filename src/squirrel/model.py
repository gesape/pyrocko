import numpy as num
import sqlite3

from pyrocko import util
from pyrocko.guts import Object, String, Timestamp, Float, Int, Unicode
from pyrocko.guts_array import Array


def tsplit(t):
    if t is None:
        return None, 0.0

    seconds = num.floor(t)
    offset = t - seconds
    return int(seconds), float(offset)


def tjoin(seconds, offset, deltat):
    if seconds is None:
        return None

    if deltat < 1e-3:
        return util.hpfloat(seconds) + util.hpfloat(offset)
    else:
        return seconds + offset


class Content(Object):
    pass


class Waveform(Content):
    agency = String.T(default='FDSN', optional=True, help='Agency code (2-5)')
    network = String.T(help='Deployment/network code (1-8)')
    station = String.T(help='Station code (1-5)')
    location = String.T(optional=True, help='Location code (0-2)')
    channel = String.T(optional=True, help='Channel code (3)')
    extra = String.T(optional=True, help='Extra/custom code')

    tmin = Timestamp.T()
    tmax = Timestamp.T()

    deltat = Float.T(optional=True)

    data = Array.T(
        shape=(None,),
        dtype=num.float32,
        serialize_as='base64',
        serialize_dtype=num.dtype('<f4'),
        help='numpy array with data samples')


class Station(Content):
    agency = String.T(default='FDSN', optional=True, help='Agency code (2-5)')
    network = String.T(help='Deployment/network code (1-8)')
    station = String.T(help='Station code (1-5)')
    location = String.T(optional=True, help='Location code (0-2)')

    tmin = Timestamp.T(optional=True)
    tmax = Timestamp.T(optional=True)

    lat = Float.T()
    lon = Float.T()
    elevation = Float.T(optional=True)
    depth = Float.T(optional=True)

    description = Unicode.T(optional=True)


class Channel(Content):
    agency = String.T(default='FDSN', optional=True, help='Agency code (2-5)')
    network = String.T(help='Deployment/network code (1-8)')
    station = String.T(help='Station code (1-5)')
    location = String.T(optional=True, help='Location code (0-2)')
    channel = String.T(optional=True, help='Channel code (3)')
    extra = String.T(optional=True, help='Extra/custom code')

    tmin = Timestamp.T(optional=True)
    tmax = Timestamp.T(optional=True)

    lat = Float.T()
    lon = Float.T()
    elevation = Float.T(optional=True)
    depth = Float.T(optional=True)

    dip = Float.T(optional=True)
    azimuth = Float.T(optional=True)
    deltat = Float.T(optional=True)


class Response(Content):
    pass


class Event(Content):
    name = String.T(optional=True)
    time = Timestamp.T()
    duration = Float.T(optional=True)

    lat = Float.T()
    lon = Float.T()
    elevation = Float.T(optional=True)
    depth = Float.T(optional=True)

    magnitude = Float.T(optional=True)


class Nut(Object):
    kind = String.T()

    agency = String.T(default='FDSN', optional=True, help='Agency code (2-5)')
    network = String.T(optional=True, help='Deployment/network code (1-8)')
    station = String.T(optional=True, help='Station code (1-5)')
    location = String.T(optional=True, help='Location code (0-2)')
    channel = String.T(optional=True, help='Channel code (3)')
    extra = String.T(optional=True, help='Extra/custom code')

    tmin_seconds = Timestamp.T(optional=True)
    tmin_offset = Float.T(default=0.0, optional=True)
    tmax_seconds = Timestamp.T(optional=True)
    tmax_offset = Float.T(default=0.0, optional=True)

    deltat = Float.T(optional=True)

    file_name = String.T(optional=True)
    file_segment = Int.T(optional=True)
    file_element = Int.T(optional=True)
    file_format = String.T(optional=True)

    file_mtime = Timestamp.T(optional=True)

    content = Content.T(optional=True)

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
            self.kind, self.agency, self.network, self.station, self.location,
            self.channel, self.extra, self.tmin_seconds, self.tmin_offset,
            self.tmax_seconds, self.tmax_offset, self.deltat,
            self.file_mtime, self.file_name, self.file_segment,
            self.file_element, self.file_format)

    @property
    def tmin(self):
        return tjoin(self.tmin_seconds, self.tmin_offset, self.deltat)

    @property
    def tmax(self):
        return tjoin(self.tmax_seconds, self.tmax_offset, self.deltat)

    @property
    def waveform_kwargs(self):
        return dict(
            agency=self.agency,
            network=self.network,
            station=self.station,
            location=self.location,
            channel=self.channel,
            extra=self.extra,
            tmin=self.tmin,
            tmax=self.tmax,
            deltat=self.deltat)

    @property
    def station_kwargs(self):
        return dict(
            agency=self.agency,
            network=self.network,
            station=self.station,
            location=self.location,
            tmin=self.tmin,
            tmax=self.tmax)

    @property
    def channel_kwargs(self):
        return dict(
            agency=self.agency,
            network=self.network,
            station=self.station,
            location=self.location,
            channel=self.channel,
            extra=self.extra,
            tmin=self.tmin,
            tmax=self.tmax,
            deltat=self.deltat)

    @property
    def event_kwargs(self):
        return dict(
            time=self.tmin,
            duration=(self.tmax - self.tmin) or None)


def make_waveform_nut(**kwargs):
    return Nut(
        kind='waveform',
        **kwargs)


def make_station_nut(**kwargs):
    return Nut(
        kind='station',
        **kwargs)


def make_channel_nut(**kwargs):
    return Nut(
        kind='channel',
        **kwargs)


def make_event_nut(**kwargs):
    return Nut(
        kind='event',
        **kwargs)


class Squirrel(object):
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        c = self.conn.cursor()
        c.execute(Nut.sql_create_table)
        self.conn.commit()

    def dig(self, nuts):
        c = self.conn.cursor()
        c.executemany(Nut.sql_insert, [nut.values() for nut in nuts])
        self.conn.commit()

    def undig(self, filename, segment, mtime, content):
        sql_select = 'SELECT * FROM nuts WHERE file_name = ? AND file_segment = ?'
        c = self.conn.cursor()
        for xx in c.execute(sql_select, (filename, segment)):
            print xx

    def add_nut(self, nut):
        c = self.conn.cursor()
        c.execute(Nut.sql_insert, nut.values())
        self.conn.commit()

    def add_nuts(self, filename):
        from pyrocko.squirrel import io
        for nut in io.iload(filename, format='detect', content=[]):
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


if False:
    sq = Squirrel()
    sq.add('/path/to/data')
#    station = sq.add(Station(...))
#    waveform = sq.add(Waveform(...))

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

