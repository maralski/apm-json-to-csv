import json
import glob
import argparse
import logging
import time
import csv
from collections import defaultdict


def make_parser():
    parser = argparse.ArgumentParser(
        description='''Parse JSON files from APM sizing queries into CSV for Distributed Tracing Inget Estimator'''
    )
    parser.add_argument('--json-dir', '-j', type=str, default='.', required=False,
                        help='directory containing json files to parse')
    parser.add_argument('--csv-file', '-c', type=str, default='data.csv', required=False,
                        help='file in which to output CSV')

    return parser
    

class LTFormatter(logging.Formatter):
    converter = time.localtime


def setup_logging():
    formatter = LTFormatter('{asctime} {levelname:<8s} {message}', style='{')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)


def read_json(fname):
    with open(fname, "r") as f:
        data = json.loads(f.read())
    return data


def main(args):
    concat = []
    fnames = glob.glob("?.json")

    records = defaultdict(dict)

    for fname in fnames:
        data = read_json(fname)
        for x in data[0]["facets"][0]["timeSeries"]:
            records[x['beginTimeSeconds']]['webtransactions'] = x['results'][0]['count']                
        for x in data[0]["facets"][1]["timeSeries"]:
            records[x['beginTimeSeconds']]['instances'] = x['results'][0]['count']

        # d1 = [dict(timestamp=x['beginTimeSeconds'], webtransactions=x['results'][0]['count']) for x in data[0]["facets"][0]["timeSeries"]]
        # d2 = [dict(timestamp=x['beginTimeSeconds'], instances=x['results'][0]['count']) for x in data[0]["facets"][1]["timeSeries"]]

    # import code; code.interact(local=locals())

    logging.info('read %d json files from the json directory', len(fnames))

    with open(args.csv_file, 'w', newline='', encoding='utf-8') as fh:
        fieldnames = ['timestamp', 'webtransactions', 'instances']
        cwriter = csv.DictWriter(fh, fieldnames=fieldnames)
        cwriter.writeheader()
        cwriter.writerows(
            [{ fieldnames[0]: x, fieldnames[1]: records[x][fieldnames[1]], fieldnames[2]: records[x][fieldnames[2]] } for x in sorted(records)]
        )

    logging.info(f'finished writing to CSV {args.csv_file}')


if __name__ == '__main__':
    parser = make_parser()
    args = parser.parse_args()
    setup_logging()
    main(args)