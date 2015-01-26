
def provided_formats():
    return ['sac']


def detect(first512):
    from pyrocko import sac

    if sac.detect(first512):
        return 'sac'
    else:
        return None
