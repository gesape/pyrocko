from pyrocko.squirrel import model

def provided_formats():
    return ['mseed']


def detect(first512):
    from pyrocko import mseed

    if mseed.detect(first512):
        return 'mseed'
    else:
        return None


def iload(format, filename, segment, mtime, want):
    assert format == 'mseed'

    from pyrocko import mseed

    load_data = 'waveforms' in want

    for itr, tr in enumerate(mseed.iload(filename, load_data=load_data)):

        nut = model.Nut(
            kind='waveform',
            agency=('', 'FDSN')[tr.network != ''],
            network=tr.network,
            station=tr.station,
            location=tr.location,
            channel=tr.channel,
            tmin = tr.tmin,
            tmax = tr.tmax,
            deltat = tr.deltat,
            file_name=filename,
            file_format=format,
            file_segment=0,
            file_element=itr,
            file_mtime=mtime)

        yield nut, tr
