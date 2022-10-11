'''
Copyright (C) 2022 Peter Elliott <pelliott@serenityos.org>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
'''
import csv
import json
import argparse
import sys
import collections

Options = collections.namedtuple('Options', ('ifile', 'ofile', 'iformat', 'oformat'))

def parse_opts(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('inputfile', help='a file to read data from', default='-', type=str)
    parser.add_argument('-o', '--output', help='the file to output processed data to', default='-')
    parser.add_argument('--iformat', help='the input format', choices=('csv', 'json'))
    parser.add_argument('--oformat', help='the output format', choices=('csv', 'json'))
    parser.add_argument('--json', help='treat input and output as json', action='store_true')
    parser.add_argument('--csv', help='treat input and output as csv', action='store_true')
    parsed = parser.parse_args()

    inputfile = parsed.inputfile
    outputfile = parsed.output
    iformat = None
    oformat = None

    if inputfile == '-':
        inputfile = '/dev/stdin'
    elif inputfile.endswith('.json'):
        iformat = 'json'
    elif inputfile.endswith('.csv'):
        iformat = 'csv'

    if outputfile == '-':
        outputfile = '/dev/stdin'
    elif outputfile.endswith('.json'):
        oformat = 'json'
    elif outputfile.endswith('.csv'):
        oformat = 'csv'

    if parsed.json:
        iformat = 'json'
        oformat = 'json'

    if parsed.csv:
        iformat = 'csv'
        oformat = 'csv'

    if parsed.iformat:
        iformat = parsed.iformat

    if parsed.oformat:
        oformat = parsed.oformat

    if iformat is None:
        print("can't infer input format please specify with --iformat")
        exit(2)

    if oformat is None:
        print("can't infer output format please specify with --oformat")
        exit(2)

    return Options(ifile=inputfile, ofile=outputfile, iformat=iformat, oformat=oformat)

class Metadata:
    def __init__(self):
        self.fields = None

def dict_csv_reader(filename, metadata):
    with open(filename) as f:
        reader = csv.DictReader(f)
        metadata.fields = reader.fieldnames
        yield
        yield from reader

def json_field_names(data):
    fields = set()
    for row in data:
        for key in row.keys():
            fields.add(key)
    return list(fields)

def dict_json_reader(filename, metadata):
    with open(filename) as f:
        data = json.load(f)
    metadata.fields = json_field_names(data)
    yield
    yield from data

def anon_csv_reader(filename, metadata):
    with open(filename) as f:
        reader = csv.reader(f)
        yield
        yield from reader

def anon_json_reader(filename, metadata):
    with open(filename) as f:
        data = json.load(f)
    yield
    yield from data

readers = {
    ('csv', False): dict_csv_reader,
    ('json', False): dict_json_reader,
    ('csv', True): anon_csv_reader,
    ('json', True): anon_json_reader,
}

def dict_csv_writer(filename, metadata, iter):
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, metadata.fields)
        writer.writeheader()
        for row in iter:
            writer.writerow(row)

def dict_json_writer(filename, metadata, iter):
    with open(filename, 'w') as f:
        json.dump(list(iter), f)

def anon_csv_writer(filename, metadata, iter):
    with open(filename, 'w') as f:
        writer = csv.writer(f, fields)
        for row in iter:
            writer.writerow(row)

def anon_json_writer(filename, metadata, iter):
    with open(filename, 'w') as f:
        json.dump(list(iter), f)

writers = {
    ('csv', False): dict_csv_writer,
    ('json', False): dict_json_writer,
    ('csv', True): anon_csv_writer,
    ('json', True): anon_json_writer,
}

def do_steps(steps, row):
    for step in steps:
        row = step(row)
        if row is None:
            break
    return row

def process_data(infile, outfile, reader, writer, steps):
    metadata = Metadata()
    r = reader(infile, metadata)
    next(r)
    writer(outfile, metadata,
           (row for row in
            (do_steps(steps, row) for row in r)
            if row is not None))

def run(steps, anon_cols=False, description='A commamancer script (you can change this description'):
    opts = parse_opts(description)

    process_data(
        opts.ifile,
        opts.ofile,
        readers[(opts.iformat, anon_cols)],
        writers[(opts.oformat, anon_cols)],
        steps
    )
