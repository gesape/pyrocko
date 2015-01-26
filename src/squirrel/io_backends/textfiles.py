
def provided_formats():
    return ['pyrocko_stations'] 


def detect_pyrocko_stations(first512):
    for line in first512.splitlines():
        t = line.split(None, 5)
        if len(t) in (5, 6):
            if len(t[0].split('.')) != 3:
                return False
            
            try:
                lat, lon, ele, dep = map(float, t[1:5])
                if lat < -90. or 90 < lat:
                    return False
                if lon < -180. or 180 < lon:
                    return False

                return True

            except:
                raise
                return False

    return False


def detect(first512):
    if detect_pyrocko_stations(first512):
        return 'pyrocko_stations'

    return None
