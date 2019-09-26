import re
import datetime
import pytz

'''
We always use UTC as timezone to avoid any potential problems!
We only concern S/E/R/D records.
'''


def timestr2unix(s, fmt='%m/%d/%Y %H:%M:%S'):
    t = pytz.utc.localize(datetime.datetime.strptime(s, fmt))
    return int(t.timestamp()), t


class record:
    __ignore_pattern = re.compile(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2};[^SRDE]')
    __pattern = re.compile(
        r'(?P<date>\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2});(?P<type>.);(?P<jobid>-?\d+)\..*?;(?P<attr>.*)')
    __attr_pattern = re.compile(r'([^= ]+)=([^ ]+)')

    date: int = 0
    rectype: str = None
    jobid: int = 0
    user: str = None
    group: str = None
    queue: str = None
    ncpus: int = 0

    def __init__(s, line):
        if s.__class__.__ignore_pattern.match(line):
            return
        m = s.__class__.__pattern.match(line)
        if not m:
            raise ValueError(line)

        s.date = timestr2unix(m.group('date'))[0]
        s.rectype = m.group('type')
        s.jobid = int(m.group('jobid'))
        attrs = {i.group(1): i.group(2) for i in s.__class__.__attr_pattern.finditer(m.group('attr'))}
        s.user = attrs['user'] if 'user' in attrs else None
        s.group = attrs['group'] if 'group' in attrs else None
        s.queue = attrs['queue'] if 'queue' in attrs else None
        s.ncpus = int(attrs['Resource_List.ncpus']) if 'Resource_List.ncpus' in attrs else 0

    def __str__(s):
        return '{} {} {} u={} g={} q={} ncpus={}'.format(
            datetime.datetime.utcfromtimestamp(s.date),
            s.rectype, s.jobid, s.user, s.group, s.queue, s.ncpus
        )
