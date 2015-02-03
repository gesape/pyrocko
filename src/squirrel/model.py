import numpy as num
import sqlite3

from pyrocko import util
from pyrocko.guts import Object, String, Timestamp, Float, Int, Unicode
from pyrocko.guts_array import Array


def str_or_none(x):
    if x is None:
        return None
    else:
        return str(x)


def float_or_none(x):
    if x is None:
        return None
    else:
        return float(x)


def int_or_none(x):
    if x is None:
        return None
    else:
        return int(x)


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
            file_name text,
            file_segment int,
            file_element int,
            file_format text,
            file_mtime float)'''

    sql_create_index = '''CREATE INDEX nuts_file_name_index ON nuts (
            file_name)'''

    sql_insert = 'INSERT INTO nuts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

    def __init__(
            self,
            kind='',
            agency='',
            network=None,
            station=None,
            location=None,
            channel=None,
            extra=None,
            tmin_seconds=None,
            tmin_offset=0.0,
            tmax_seconds=None,
            tmax_offset=0.0,
            deltat=None,
            file_name=None,
            file_segment=None,
            file_element=None,
            file_format=None,
            file_mtime=None,
            tmin=None,
            tmax=None):

        if tmin is not None:
            tmin_seconds, tmin_offset = tsplit(tmin)

        if tmax is not None:
            tmax_seconds, tmax_offset = tsplit(tmax)

        self.kind = str(kind)
        self.agency = str(agency)
        self.network = str_or_none(network)
        self.station = str_or_none(station)
        self.location = str_or_none(location)
        self.channel = str_or_none(channel)
        self.extra = str_or_none(extra)
        self.tmin_seconds = int_or_none(tmin_seconds)
        self.tmin_offset = float(tmin_offset)
        self.tmax_seconds = int_or_none(tmax_seconds)
        self.tmax_offset = float(tmin_offset)
        self.deltat = float_or_none(deltat)
        self.file_name = str_or_none(file_name)
        self.file_segment = int_or_none(file_segment)
        self.file_element = int_or_none(file_element)
        self.file_format = str_or_none(file_format)
        self.file_mtime = int_or_none(file_mtime)
        Object.__init__(self, init_props=False)

    def values(self):
        return (
            self.kind, self.agency, self.network, self.station, self.location,
            self.channel, self.extra, self.tmin_seconds, self.tmin_offset,
            self.tmax_seconds, self.tmax_offset, self.deltat,
            self.file_name, self.file_segment,
            self.file_element, self.file_format, self.file_mtime)

    @classmethod
    def from_values(cls, values):
        o = cls(*values)
        return o

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
        c.execute(Nut.sql_create_index)
        self.conn.commit()
        c.close()

    def dig(self, nuts):
        c = self.conn.cursor()
        c.executemany(Nut.sql_insert, [nut.values() for nut in nuts])
        self.conn.commit()
        c.close()

    def undig(self, filename=None, segment=None, mtime=None, content=None):
        sql_where = []
        args = []
        if filename is not None:
            sql_where.append('file_name = ?')
            args.append(filename)
            if segment is not None:
                sql_where.append('file_segment = ?')
                args.append(segment)

        sql = 'SELECT * FROM nuts WHERE %s' % ' AND '.join(sql_where)
        nuts = [Nut.from_values(values) for values in
                self.conn.execute(sql, args)]

        if mtime is None:
            return nuts
        else:

            uptodate = [nut for nut in nuts if nut.file_mtime == mtime]
            if len(uptodate) != len(nuts):
                sql_where.append('file_mtime != ?')
                args.append(mtime)
                sql = 'DELETE FROM nuts WHERE %s' % ' AND '.join(sql_where)
                self.conn.execute(sql, args)
                self.conn.commit()

            return uptodate

    def undig_content(self, nut):
        return None

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

