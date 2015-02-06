from pyrocko.squirrel import model


def provided_formats():
    return ['datacube']


def detect(first512):
    from pyrocko import datacube

    if datacube.detect(first512):
        return 'datacube'
    else:
        return None


def iload(format, filename, segment, content):
    assert format == 'datacube'

    from pyrocko import datacube

    load_data = 'waveform' in content

    for itr, tr in enumerate(datacube.iload(filename, load_data=load_data)):

        nut = model.Nut(
            file_segment=0,
            file_element=itr,
            kind='waveform',
            agency=('', 'FDSN')[tr.network != ''],
            network=tr.network,
            station=tr.station,
            location=tr.location,
            channel=tr.channel,
            tmin = tr.tmin,
            tmax = tr.tmax,
            deltat = tr.deltat)

        if 'waveform' in content:
            nut.content = model.Waveform(
                data=tr.ydata,
                **nut.waveform_kwargs)

        yield nut
