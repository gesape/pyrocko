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
            'CREATE TABLE IF NOT EXISTS nuts %s' % model.Nut.sql_columns)

        c.execute(
            'CREATE INDEX IF NOT EXISTS nuts_file_name_index '
            'ON nuts (file_name)')

        c.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS nuts_file_element_index '
            'ON nuts (file_name, file_segment, file_element)')

        self.conn.commit()
        c.close()

    def dig(self, nuts):
        c = self.conn.cursor()
        c.executemany(
            'INSERT OR REPLACE INTO nuts VALUES %s' %
            model.Nut.sql_placeholders,
            [nut.values() for nut in nuts])

        c.close()
        self._need_commit = True

    def undig(self, filename, segment=None, mtime=None):
        sql_where = []
        args = []
        if filename is not None:
            sql_where.append('file_name = ?')
            args.append(filename)
            if segment is not None:
                sql_where.append('file_segment = ?')
                args.append(segment)

        if sql_where:
            sql = 'SELECT * FROM nuts WHERE %s' % ' AND '.join(sql_where)
        else:
            sql = 'SELECT * FROM nuts'

        nuts = [model.Nut(values_nocheck=values) for values in
                self.conn.execute(sql, args)]

        if not nuts:
            return nuts

        if mtime is None:
            mtime = max(nut.file_mtime for nut in nuts)

        uptodate = [nut for nut in nuts if nut.file_mtime == mtime]

        if filename is not None and len(uptodate) != len(nuts):
            print 'del', filename
            sql_where.append('file_mtime != ?')
            args.append(mtime)
            sql = 'DELETE FROM nuts WHERE %s' % ' AND '.join(sql_where)
            self.conn.execute(sql, args)
            self._need_commit = True

        return uptodate

    def delete_outdated(filename, mtime):
        sql = 'DELETE FROM nuts WHERE file_name = ? and mtime != ?'
        self.conn.execute(sql, (filename, mtime))
        self._need_commit = True

    def undig_many(self, filenames):
        self.conn.execute(
            'CREATE TEMP TABLE undig_many (file_name text)')

        self.conn.executemany(
            'INSERT INTO temp.undig_many VALUES (?)', ((s,) for s in filenames))

        #self.conn.commit()

        sql = 'SELECT * FROM nuts NATURAL JOIN temp.undig_many'

        sql = '''
            SELECT * FROM temp.undig_many 
            LEFT OUTER JOIN nuts ON temp.undig_many.file_name = nuts.file_name 
            ORDER BY temp.undig_many.rowid
        '''

        nuts = []
        fn = None
        for values in self.conn.execute(sql):
            if fn is not None and values[0] != fn:
                yield fn, nuts
                nuts = []

            if values[13] is not None:
                nuts.append(model.Nut(values_nocheck=values[1:]))

            fn = values[0]

        if fn is not None:
            yield fn, nuts

        self.conn.execute(
            'DROP TABLE temp.undig_many')

        #self.conn.commit()

    def commit(self):
        if self._need_commit:
            self.conn.commit()
            self._need_commit = False

    def undig_raw(self):
        sql = 'SELECT * FROM nuts'
        for x in self.conn.execute(sql):
            yield x

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
