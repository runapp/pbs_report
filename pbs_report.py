#!/usr/bin/env python3

import os
import argparse
import datetime
import traceback
import pytz
import job_state as _job_state
import record as _record
record = _record.record
job_state = _job_state.job_state
timestr2unix = _record.timestr2unix


def smart_time_parser(s, reftime):
    l, r = len(s) if s else 0, reftime
    if l == 8:
        return timestr2unix(s, '%Y%m%d')
    elif l == 6:
        return timestr2unix('20'+s, '%Y%m%d')
    elif l == 4:
        return timestr2unix('%04d%s' % (r.year, s), '%Y%m%d')
    elif l == 2:
        return timestr2unix('%04d%02d%s' % (r.year, r.month, s), '%Y%m%d')
    elif l == 0:
        return timestr2unix('%04d%02d%02d' % (r.year, r.month, r.day), '%Y%m%d')
    else:
        raise ValueError("Not any format of yyyymmdd, yymmdd, mmdd, dd.")


def process(job_states, fn, def_time, user_filter, group_filter, queue_filter):
    for l in open(fn):
        r = record(l)
        if r.rectype is None:
            continue
        if (user_filter and r.user not in user_filter or
            group_filter and r.group not in group_filter or
                queue_filter and r.queue not in queue_filter):
            continue
        if r.jobid not in job_states:
            job_states[r.jobid] = job_state(def_time)
        try:
            job_states[r.jobid].push(r)
        except _job_state.StateError:
            traceback.print_exc()
            print(f"jobid is {r.jobid}")
            # exit(1)


def main(start_time, end_time_str, filelist, opts):
    user_filter, group_filter, queue_filter, queue_ignore = map(
        frozenset, [opts.only_user, opts.only_group, opts.only_queue, opts.ignore_queue])
    job_states = {}
    for i in filelist:
        if os.path.isfile(i):
            #print(f"Processing: {i}")
            process(job_states, i, start_time, user_filter, group_filter, queue_filter)
        else:
            #print(f"File not found: {i}")
            True
    fake_E_rec = end_time_str+';E;-1.dummy;'

    user_stat, group_stat = {}, {}
    total_cputime = 0
    for jobid, js in job_states.items():
        if js.state not in 'DER':
            js.push(record(fake_E_rec))
        if opts.verbose or len(js.queues) > 1 and opts.debug_multiqueue:
            print('{:10}  {}'.format(jobid, js))
        js.queues = frozenset(js.queues-queue_ignore)
        if opts.user:
            try:
                user_stat[(js.user, js.group, js.queues)] += js.cputime
            except KeyError:
                user_stat[(js.user, js.group, js.queues)] = js.cputime
            except TypeError:
                print(js.queues)
        if opts.group:
            try:
                group_stat[js.group] += js.cputime
            except KeyError:
                group_stat[js.group] = js.cputime

        total_cputime += js.cputime

    print(f"Total CPU Time is: {total_cputime}")
    if opts.user:
        print("Total CPU Time per User:")
        for k, v in user_stat.items():
            if k[0] is None:
                k = ('None', 'None', frozenset())
            print("{:13} {:10} {:15} {}".format(k[0], k[1], v, ','.join(k[2])))
    if opts.group:
        print("Total CPU Time per Group:")
        for k, v in group_stat.items():
            if k is None:
                k = 'None'
            print("{:10} {:15}".format(k, v))


def pre_main():
    parser = argparse.ArgumentParser(description='Statistic information of PBS accounting data.')
    parser.add_argument('start_date',
                        help='Start date. Format is [[yy]yy]mmdd. Year is set to current year if not specified.')
    parser.add_argument('end_date', nargs='?', help='Same as start_date. If not specified, treat as today.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Print all job stats at the end.')
    parser.add_argument('-u', '--user', action='store_true', help='Group by user.')
    parser.add_argument('-g', '--group', action='store_true', help='Group by group.')
    parser.add_argument('-U', '--only-user', action='append', help='Only count in specified user.', default=[])
    parser.add_argument('-G', '--only-group', action='append', help='Only count in specified group.', default=[])
    parser.add_argument('-Q', '--only-queue', action='append', help='Only count in specified queue.', default=[])
    parser.add_argument('-R', '--ignore-queue', action='append',
                        help='Treat specified queue as router queues (do not include them in the final result).', default=[])
    parser.add_argument('-D', '--debug-multiqueue', action='store_true',
                        help='Print job stats for multi-queue jobs (which is considered unusual, and might be a hint for jobid conflicts).')
    opts = parser.parse_args()
    # print(opts)

    nowtime = pytz.utc.localize(datetime.datetime.now())
    start_time = smart_time_parser(opts.start_date, nowtime)
    end_time = smart_time_parser(opts.end_date, nowtime)
    filelist = []
    t = start_time[1]
    while t <= end_time[1]:
        filelist.append('%04d%02d%02d' % (t.year, t.month, t.day))
        t += datetime.timedelta(days=1)

    end_time_str = (end_time[1]+datetime.timedelta(hours=23, minutes=59, seconds=59)).strftime('%m/%d/%Y %H:%M:%S')

    main(start_time[0], end_time_str, filelist, opts)


pre_main()
