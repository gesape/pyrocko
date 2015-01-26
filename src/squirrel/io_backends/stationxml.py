
def provided_formats():
    return ['stationxml']


def detect(first512):
    if first512.find('<FDSNStationXML') != -1:
        return 'stationxml'

    return None
