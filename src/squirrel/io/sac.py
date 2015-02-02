from pyrocko.squirrel import model

km = 1000.


def provided_formats():
    return ['sac']


def detect(first512):
    from pyrocko import sac

    if sac.detect(first512):
        return 'sac'
    else:
        return None


def agg(*ds):
    out = {}
    for d in ds:
        out.update(d)

    return out


def nonetoempty(x):
    if x is None:
        return x
    else:
        return x.strip()


def iload(format, filename, segment, mtime, content):
    assert format == 'sac'

    from pyrocko import sac

    load_data = 'waveform' in content

    s = sac.SacFile(filename, load_data=load_data)

    source = dict(
        file_name=filename,
        file_format=format,
        file_segment=0,
        file_mtime=mtime)

    codes = dict(
        network=nonetoempty(s.knetwk),
        station=nonetoempty(s.kstnm),
        location=nonetoempty(s.khole),
        channel=nonetoempty(s.kcmpnm))

    codes['agency'] = ('', 'FDSN')[codes['network'] != '']

    tmin = s.get_ref_time() + s.b
    tmax = tmin + s.delta * (s.npts-1)

    tspan = dict(
        tmin=tmin,
        tmax=tmax,
        deltat=s.delta)

    inut = 0
    nut = model.make_waveform_nut(
        file_element=inut,
        **agg(codes, tspan, source))

    if 'waveform' in content:
        nut.content = model.Waveform(
            data=s.data[0],
            **nut.waveform_kwargs)

    yield nut
    inut += 1

    if None not in (s.stla, s.stlo):
        position = dict(
            lat=s.stla,
            lon=s.stlo,
            elevation=s.stel,
            depth=s.stdp)

        nut = model.make_station_nut(
            file_element=inut,
            **agg(codes, tspan, source))

        if 'station' in content:
            nut.content = model.Station(
                **agg(position, nut.station_kwargs))

        yield nut
        inut += 1

        dip = None
        if s.cmpinc is not None:
            dip = s.cmpinc - 90.

        nut = model.make_channel_nut(
            file_element=inut,
            **agg(codes, tspan, source))

        if 'channel' in content:
            nut.content = model.Channel(
                azimuth=s.cmpaz,
                dip=dip,
                **agg(position, nut.channel_kwargs))

        yield nut
        inut += 1

    if None not in (s.evla, s.evlo, s.o):
        etime = s.get_ref_time() + s.o
        depth = None
        if s.evdp is not None:
            depth = s.evdp  # * km  #  unclear specs

        nut = model.make_event_nut(
            file_element=inut,
            tmin=etime,
            tmax=etime,
            **agg(source))

        if 'event' in content:
            nut.content = model.Event(
                name=nonetoempty(s.kevnm),
                lat=s.evla,
                lon=s.evlo,
                depth=depth,
                magnitude=s.mag,
                **nut.event_kwargs)

        yield nut
        inut += 1
