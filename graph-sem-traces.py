#!/usr/bin/env python
"""Given a trace from traces/, generate a plot.

Misc notes:

    traces/size-13421772400-download-to-dev-null-1.trace - default window size

"""
import argparse
import dateutil.parser
import json
from collections import namedtuple

import matplotlib.pyplot as p


RawRecord = namedtuple('RawRecord', ['timestamp', 'thread', 'message'])

def graph_trace(args):
    p.grid(True)
    p.xlabel('Time')
    p.ylabel('Sequence Number')
    p.title("Sliding Window Semaphore Acquisition/Release")
    for tracefile in args.tracefile:
        parsed = parse_trace_file(tracefile)
        parsed = normalize_time(parsed)
        if args.plot_type == 'all':
            plot_acquires(parsed)
            plot_released(parsed)
            plot_release_requests(parsed)
            plot_trigger_releases(parsed)
            continue
        if args.plot_type == 'acquire':
            plot_acquires(parsed)
            plot_released(parsed)
        elif args.plot_type == 'release':
            plot_released(parsed)
            plot_release_requests(parsed)
            plot_trigger_releases(parsed)
    p.legend()
    if args.output_file:
        p.savefig(args.output_file)
    else:
        p.show()


def plot_acquires(parsed):
    acquires = list(sorted(filter_events(parsed, event_type='acquire'),
                           key=lambda x: x.timestamp))
    x_vals = [m.timestamp for m in acquires]
    y_vals = [m.message['sequence_number'] for m in acquires]
    plot = p.plot(x_vals, y_vals, 'b.', label='Acquire')


def plot_released(parsed):
    events = filter_events(parsed, event_type='release')
    events = filter_releases(events, immediate=True)
    released = list(sorted(events, key=lambda x: x.timestamp))
    x_vals = [m.timestamp for m in released]
    y_vals = [m.message['sequence_number'] for m in released]
    p.plot(x_vals, y_vals, 'r.', label='Release Chain')


def plot_release_requests(parsed):
    events = filter_events(parsed, event_type='release')
    events = filter_releases(events, immediate=False)
    released = list(sorted(events, key=lambda x: x.timestamp))
    x_vals = [m.timestamp for m in released]
    y_vals = [m.message['sequence_number'] for m in released]
    p.plot(x_vals, y_vals, 'g.', label='Release Request')


def plot_trigger_releases(parsed):
    # Plot the release events that trigger a chain of releases.
    events = filter_events(parsed, event_type='release')
    events = filter_releases(events, immediate=True, chain=False)
    released = list(sorted(events, key=lambda x: x.timestamp))
    x_vals = [m.timestamp for m in released]
    y_vals = [m.message['sequence_number'] for m in released]
    p.plot(x_vals, y_vals, 'k.', label='Release Trigger')


def normalize_time(parsed):
    min_time = min(parsed, key=lambda x: x.timestamp).timestamp
    return [RawRecord(r.timestamp - min_time, r.thread, r.message)
            for r in parsed]


def filter_events(messages, event_type):
    return (m for m in messages if m.message['event_type'] == event_type)

def filter_releases(messages, immediate, chain=None):
    result = (
        m for m in messages if m.message['immediate_release'] == immediate)
    if chain is not None:
        return (
            r for r in result
            if r.message.get('chain_release', False) == chain
        )
    else:
        return result


def parse_trace_file(filename):
    # Sample line:
    # 2018-07-25 11:04:18,630 - Thread-1 - s3transfer.tracer - DEBUG - {"requested_time": "2018-07-25 11:04:18.630345", "sem_tag": 0, "event_type": "acquire", "sequence_number": 0}
    messages = []
    with open(filename) as f:
        for line in f:
            parsed_line = parse_line(line)
            messages.append(parsed_line)
    return messages


def parse_line(line):
    parts = line.split(' - ')
    date = parts[0]
    thread = parts[1]
    message = parts[-1]
    parsed_date = float(dateutil.parser.parse(date).strftime('%s.%f'))
    parsed_message = json.loads(message.strip())
    return RawRecord(parsed_date, thread, parsed_message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('tracefile', nargs='+')
    parser.add_argument('-p', '--plot-type', choices=['acquire',
                                                      'release', 'all'],
                        default='acquire')
    parser.add_argument('-o', '--output-file', help='Save diagram to file.')
    args = parser.parse_args()
    graph_trace(args)


if __name__ == '__main__':
    main()
