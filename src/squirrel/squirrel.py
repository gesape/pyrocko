import time
from pyrocko.squirrel import model
import sqlite3


class Squirrel(object):
    def __init__(self, database=':memory:'):
        self.conn = sqlite3.connect(database)
        self.conn.text_factory = str
        self._initialize_db()
        self._need_commit = False

    def _initialize_db(self):
        c = self.conn.cursor()
        c.execute(
            '''CREATE TABLE IF NOT EXISTS files (
                file_name text PRIMARY KEY,
                file_format text,
                file_mtime float)''')

        c.execute(
            '''CREATE TABLE IF NOT EXISTS nuts (
                file_id int,
                file_segment int,
                file_element int,
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
                PRIMARY KEY (file_id, file_segment, file_element))''')

        c.execute(
            '''CREATE INDEX IF NOT EXISTS nuts_file_id_index
                ON nuts (file_id)''')

        c.execute(
            '''CREATE TRIGGER IF NOT EXISTS delete_nuts 
                BEFORE DELETE ON files FOR EACH ROW
                BEGIN
                  DELETE FROM nuts where file_id = old.rowid;
                END''')

        self.conn.commit()
        c.close()

    def dig(self, nuts):
        if not nuts:
            return

        c = self.conn.cursor()
        by_files = {}
        for nut in nuts:
            k = nut.file_name, nut.file_format, nut.file_mtime
            if k not in by_files:
                by_files[k] = []

            by_files[k].append(nut)

        for k, file_nuts in by_files.iteritems():
            c.execute('DELETE FROM files WHERE file_name = ?', k[0:1])
            c.execute('INSERT INTO files VALUES (?,?,?)', k)
            file_id = c.lastrowid
            c.executemany(
                'INSERT INTO nuts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                [(file_id, nut.file_segment, nut.file_element, nut.kind,
                  nut.agency, nut.network, nut.station, nut.location,
                  nut.channel, nut.extra, nut.tmin_seconds,
                  nut.tmin_offset, nut.tmax_seconds, nut.tmax_offset,
                  nut.deltat) for nut in file_nuts])

        self._need_commit = True
        c.close()

    def undig(self, filename):
        sql = '''
            SELECT * FROM files 
            LEFT OUTER JOIN nuts ON files.rowid = nuts.file_id
            WHERE file_name = ?'''

        return [model.Nut(values_nocheck=row)
                for row in self.conn.execute(sql, (filename,))]

    def undig_many(self, filenames):
        self.conn.execute(
            'CREATE TEMP TABLE undig_many (file_name text)')

        self.conn.executemany(
            'INSERT INTO temp.undig_many VALUES (?)', ((s,) for s in filenames))

        sql = '''
            SELECT * FROM temp.undig_many 
            LEFT OUTER JOIN files ON temp.undig_many.file_name = files.file_name 
            LEFT OUTER JOIN nuts ON files.rowid = nuts.file_id
            ORDER BY temp.undig_many.rowid
        '''

        nuts = []
        fn = None
        t0 = time.time()
        ii = 0
        for values in self.conn.execute(sql):
            if fn is not None and values[0] != fn:
                yield fn, nuts
                nuts = []

            if values[1] is not None:
                nuts.append(model.Nut(values_nocheck=values[1:]))

            fn = values[0]

        if fn is not None:
            yield fn, nuts

        t1 = time.time()
        print t1 - t0

        self.conn.execute(
            'DROP TABLE temp.undig_many')

    def choose(self, filenames):
        self.conn.execute(
            'CREATE TEMP TABLE choosen_files (file_name text)')

        self.conn.executemany(
            'INSERT INTO temp.choosen_files VALUES (?)', ((s,) for s in filenames))

        sql = '''
            SELECT nuts.rowid FROM temp.choosen_files 
            INNER JOIN files ON temp.choosen_files.file_name = files.file_name 
            INNER JOIN nuts ON files.rowid = nuts.file_id
        '''

        t0 = time.time()
        ii = 0
        for values in self.conn.execute(sql):
            ii += 1

        t1 = time.time()
        print t1 - t0
        
        print ii

        self.conn.execute(
            'DROP TABLE temp.choosen_files')

    def commit(self):
        if self._need_commit:
            self.conn.commit()
            self._need_commit = False

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

    station = model.Station()
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
