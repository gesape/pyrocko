

def provided_formats():
    return ['datacube']


def detect(first512):
    from pyrocko import datacube

    if datacube.detect(first512):
        return 'datacube'
    else:
        return None
