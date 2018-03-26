#!/usr/bin/env python
import argparse
import json
import os
import socket
import sys
import traceback

import graphitesend

r"""
Analyze fio output and send metrics to Graphite.

An example as follows::

    $ python fio-parser.py -d /opt/fio/profiles/results -s cat-hlz-mon0.os.hlz.cat-it.co.nz

Note:
- This script has a python lib dependency, make sure running
  `pip install graphitesend` beforehand.
- The output directory should be an absolute path.
- The metrics can be defined using `-m`, metric name is a '.' separated string
  each part of which comes from job output. However, there is an exception for
  percentile, please replace '.' with ',' for the number. For example,
  '-m read.bw write.clat.percentile.99,500000'
- After handling an output file, a suffix('.bak') will be added to the file
  name to avoid duplicate handling.

"""


def get_output_files(dir_path):
    file_list = []

    for f in os.listdir(dir_path):
        file_path = os.path.join(dir_path, f)

        if (not f.startswith('.') and os.path.isfile(file_path) and
                not f.endswith('.bak')):
            file_list.append(file_path)

    return file_list


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', "--output-dir", required=True,
                        help="The absolute directory path containing fio "
                             "output files.")
    parser.add_argument('-m', "--metrics", nargs='*',
                        default=['read.iops'],
                        help="The metrics definition.")
    parser.add_argument('-s', "--host", required=True,
                        help="The host carbon is running on.")
    parser.add_argument('-p', "--port", type=int, default=2003,
                        help="The port carton is listening to.")
    parser.add_argument('--dry-run', default=False, action='store_true',
                        help="Toggle if it will really send metrics or just "
                             "return them.")
    args = parser.parse_args()

    output_files = get_output_files(args.output_dir)

    client = graphitesend.GraphiteClient(
        prefix='benchmark.ceph', graphite_server=args.host,
        graphite_port=args.port, fqdn_squash=True,
        timeout_in_seconds=10, dryrun=args.dry_run
    )

    total_count = 0

    for output_file in output_files:
        print('Processing file: %s' % output_file)

        try:
            with open(output_file, 'r') as f:
                output = json.load(f)
        except Exception as e:
            print('Error occured when open the output file %s, error: %s' %
                  (output_file, str(e)))
            continue

        jobs = output['jobs']
        timestamp = output['timestamp']
        count = 0

        try:
            for j in jobs:
                data = {}
                job_name = j['jobname']

                for m in args.metrics:
                    attribute = j
                    keys = m.split('.')
                    for k in keys:
                        if ',' in k:
                            k = k.replace(',', '.')
                        attribute = attribute.get(k)

                    data_key = '%s.%s' % (job_name, m)
                    data[data_key] = attribute

                print client.send_dict(data, timestamp=timestamp)
                count += len(data)

            new_file = '%s.bak' % output_file
            os.rename(output_file, new_file)

            print('%s metrics sent for file: %s' % (count, output_file))

            total_count += count
        except Exception as e:
            print('Error occured when retrieving/sending data to Graphite, '
                  'error: %s' % str(e))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=4, file=sys.stdout)
            continue

    print('%s metrics sent in total.' % total_count)
    client.disconnect()


if __name__ == '__main__':
    main()
