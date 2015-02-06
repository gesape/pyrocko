import time
from pyrocko.squirrel import model

Y = 60*60*24*365


def provided_formats():
    return ['stationxml']


def detect(first512):
    if first512.find('<FDSNStationXML') != -1:
        return 'stationxml'

    return None


def iload(format, filename, segment, content):
    assert format == 'stationxml'

    far_future = time.time() + 20*Y

    from pyrocko.fdsn import station as fdsn_station
    value_or_none = fdsn_station.value_or_none

    sx = fdsn_station.load_xml(filename=filename)

    inut = 0

    for network in sx.network_list:
        for station in network.station_list:
            net = network.code
            sta = station.code
            agn = ('', 'FDSN')[net != '']

            tmin = station.start_date
            tmax = station.end_date
            if tmax > far_future:
                tmax = None

            nut = model.make_station_nut(
                file_segment=0,
                file_element=inut,
                agency=agn,
                network=net,
                station=sta,
                tmin=tmin,
                tmax=tmax)

            if 'station' in content:
                nut.content = model.Station(
                    lat=station.latitude.value,
                    lon=station.longitude.value,
                    elevation=value_or_none(station.elevation),
                    **nut.station_kwargs)

            yield nut
            inut += 1

            for channel in station.channel_list:
                cha = channel.code
                loc = channel.location_code.strip()

                tmin = channel.start_date
                tmax = channel.end_date
                if tmax > far_future:
                    tmax = None

                if channel.sample_rate is not None:
                    deltat = 1.0 / channel.sample_rate.value

                nut = model.make_channel_nut(
                    file_segment=0,
                    file_element=inut,
                    agency=agn,
                    network=net,
                    station=sta,
                    location=loc,
                    channel=cha,
                    tmin=tmin,
                    tmax=tmax,
                    deltat=deltat)

                if 'channel' in content:
                    nut.content = model.Channel(
                        lat=channel.latitude.value,
                        lon=channel.longitude.value,
                        elevation=value_or_none(channel.elevation),
                        depth=value_or_none(channel.depth),
                        azimuth=value_or_none(channel.azimuth),
                        dip=value_or_none(channel.dip),
                        **nut.channel_kwargs)

                yield nut
                inut += 1
