import pytz

VIETNAM_TIMEZONE = pytz.timezone("Asia/Ho_Chi_Minh")


def to_aware(dt, tz=pytz.utc):
    """
    Convert naive datetime to aware datetime.
    """
    if dt.tzinfo is not None:
        return dt.astimezone(tz)
    return tz.localize(dt)


def to_naive(dt):
    """
    Convert aware datetime to naive datetime.
    """
    return dt.replace(tzinfo=None)


def dt_to_utc(dt):
    """
    Convert aware datetime to naive datetime.
    """
    return to_aware(dt).astimezone(pytz.utc)


def dt_to_local(dt):
    return to_aware(dt).astimezone(VIETNAM_TIMEZONE)
