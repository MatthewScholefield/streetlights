#!/usr/bin/env python3
import json
from argparse import ArgumentParser
from csv import DictReader
from glob import glob
from os.path import join


def main():
    parser = ArgumentParser()
    parser.add_argument('csv_files', nargs='*', help='all csv files to combine')
    parser.add_argument(
        '-o', '--output-file', help='Output combined json file',
        default=join('data', 'combined.json')
    )
    args = parser.parse_args()
    combined = []
    args.csv_files = args.csv_files or glob(join('data', '*.clean.csv'))
    for csv_file in args.csv_files:
        print('Loading {}...'.format(csv_file))
        with open(csv_file) as f:
            for i, row in enumerate(DictReader(f)):
                print(i + 1, '\r', flush=True, end='')
                combined.append(row)
    with open(args.output_file, 'w') as f:
        json.dump(combined, f)
    print('Wrote to', args.output_file)


if __name__ == '__main__':
    main()
