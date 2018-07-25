import argparse
import logging

import botocore.session
from s3transfer.manager import TransferManager, TransferConfig



def run_demo(args):
    configure_trace_logs(args)
    s = botocore.session.get_session()
    config = TransferConfig(max_in_memory_upload_chunks=args.window_size)
    m = TransferManager(s.create_client('s3'), config=config)
    future = m.download(args.bucket, args.key, args.filename)
    future.result()


def configure_trace_logs(args):
    if args.trace_file is None:
        print("No trace file provided, not configuring trace logs")
        return
    log = logging.getLogger('s3transfer.tracer')
    log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch = logging.FileHandler(filename=args.trace_file, mode='w')
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    log.addHandler(ch)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bucket', default='jamesls-test-sync')
    parser.add_argument('-k', '--key', default='largefile')
    parser.add_argument('-w', '--window-size', default=10, type=int)
    parser.add_argument('-f', '--filename', default='/tmp/download')
    parser.add_argument('-t', '--trace-file')
    args = parser.parse_args()
    run_demo(args)


if __name__ == '__main__':
    main()
