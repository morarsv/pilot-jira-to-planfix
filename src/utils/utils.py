import pytz
import logging
from datetime import datetime

def current_time() -> str:
    current_time_zone = datetime.now(pytz.utc)
    utc_plus_7 = current_time_zone.astimezone(pytz.timezone("Asia/Novosibirsk"))
    time = utc_plus_7.strftime("%Y-%m-%d %H:%M:%S")
    return time

class CustomFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='{', tz=None):
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.tz = tz

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.isoformat()
