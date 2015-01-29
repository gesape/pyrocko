
def provided_formats():
    return ['sac']


def detect(first512):
    from pyrocko import sac

    if sac.detect(first512):
        return 'sac'
    else:
        return None

def iload(format, filename, segment, mtime, want):
    assert format == 'sac'

    from pyrocko import sac

    load_data = 'waveforms' in want

    s = sac.SacFile(filename, load_data=load_data)
    tr = s.to_trace()
    
    kwargs = dict(
        agency=('', 'FDSN')[tr.network != ''],
        network=tr.network,
        station=tr.station,
        location=tr.location,
        channel=tr.channel,
        tmin=tr.tmin,
        tmax=tr.tmax,
        file_name=filename,
        file_format=format,
        file_segment=0,
        file_mtime=mtime)

    nut = model.Nut(
        kind='waveform',
        file_element=0,
        deltat=tr.deltat,
        content=(None, tr)['waveforms' in want]
        **kwargs)

    if None not in (s.stla, s.stlo):
        model.Station(
            tmin=tr.tmin,
            tmax=tr.tmax,
            lat=s.stla,
            lon=s.stlo,
            elevation=s.stel,
            depth=s.stdp)

        dip = None
        if s.inc is not None:
            dip = s.inc - 90.

        model.Channel(
            tmin=tr.tmin,
            tmax=tr.tmax,
            lat=s.stla,
            lon=s.stlo,
            elevation=s.stel,
            depth=s.stdp,
            azimuth=s.cmpaz,
            dip=dip)

    model.Event(
        lat=s.evla,
        lon=s.evlo,
        depth=s.evdp*km,
        magnitude=s.magnitude)

