from datetime import datetime

ENCODING = "utf-8"


def datetime_to_str(dt):
    return f"{dt.year}:{dt.month}:{dt.day}:{dt.hour}:{dt.minute}:{dt.second}"


def str_to_datetime(s):
    return datetime(*(int(part) for part in s.split(":")))


def split_header(msg):
    """Find the first empty frame and split the message there.

    Return the "headers" part, including the empty frame and the rest.

    :param msg: List of byte chunks (from ZMQ frames).
    :type msg: list
    :rtype: (list, list)
    """
    try:
        empty_frame_index = msg.index(b"")
    except ValueError:
        return None, msg
    split_index = empty_frame_index + 1
    return msg[:split_index], msg[split_index:]
