import record as _record
record = _record.record


class StateError(Exception):
    pass


class job_state:
    def __init__(s, def_time):
        s.state = '0'
        s.cputime = 0
        s.last_ncpus = None
        s.last_stime = def_time
        s.has_orphan = False
        s.records = []
        s.user, s.group = None, None
        s.queues = set()

    def __update_info(s, rec: record):
        '''Return if data is valid and update was successful. If `None` returned, the caller should do no extra processing.'''
        if rec.user is not None:
            s.user = rec.user
        if rec.group is not None:
            s.group = rec.group
        if rec.queue is not None and rec.queue not in s.queues:
            s.queues.add(rec.queue)
        if rec.rectype == 'S':
            if s.state in '0RED':  # E here is for multiple items shares same jobid.
                if s.has_orphan:
                    print("Warning: Orphan D/E/R record found for job {}. May leave out cpu times.".format(rec.jobid))
                if True or rec.group == rec.queue:
                    s.last_ncpus, s.last_stime = rec.ncpus, rec.date
                else:
                    print('Ignoring job {} running in queue {}, owned by {}/{}'.format(rec.jobid, rec.queue, rec.user, rec.group))
                    s.last_ncpus, s.last_stime = 0, 0
                return True
            else:
                return False
        elif s.state == '0':
            s.has_orphan = True
            if rec.ncpus is not None:
                s.cputime += rec.ncpus*(rec.date-s.last_stime)
                return True
            else:
                return None
        elif rec.rectype in 'DE':
            if s.state == 'S':
                try:
                    s.cputime += s.last_ncpus*(rec.date-s.last_stime)
                except Exception as e:
                    print(str(s)+'   lncpu={} rdate={} ltime={}'.format(s.last_ncpus, rec.date, s.last_stime))
                    print('history:')
                    print('\n'.join(str(i) for i in s.records))
                    raise e
                return True
            elif rec.rectype == 'D' or (s.state == 'D' and rec.rectype == 'E'):
                return True
            else:
                return False
        elif rec.rectype == 'R':
            if s.state == 'S':
                s.cputime += s.last_ncpus*(rec.date-s.last_stime)
            return True
        else:
            print('Encountered record type not in SRDE: {}'.format(rec))
            return None

    def push(s, rec: record):
        '''Push a new record, and update accounting info.'''
        t = s.__update_info(rec)
        if t == True:
            s.state = rec.rectype
        elif t == False:
            raise StateError('self: '+str(s)+'\nrec history:\n'+'\n'.join(str(i) for i in s.records)+'\n'+str(rec))
        s.records.append(rec)

    def __str__(s):
        return '{} {:10} {:10} {:10}  {}'.format(s.state, str(s.user), str(s.group), str(s.cputime), s.queues)
