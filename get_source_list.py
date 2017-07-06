#!/usr/bin/env python

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3

import argparse

if __name__ == "__main__":

    #
    # Handle all command line stuff
    #
    parser = argparse.ArgumentParser(
        description='Extract catalog of associated sources')
    parser.add_argument(
        'database_fn', type=str, #nargs=1,
        metavar='data.base',
        help='database filename')
    parser.add_argument('--nmin', dest='n_min_phot', type=int,
                        default=1, help='minimum number of photometry datapoints')
    parser.add_argument('--nmax', dest='n_max_phot', type=int,
                        default=-1, help='maximum number of photometry datapoints')
    parser.add_argument('--out', dest='output', type=str,
                        default=sys.stdout, help='output filename')
    args = parser.parse_args()


    #
    # open database
    #
    db = sqlite3.connect(args.database_fn)
    curs = db.cursor()

    sql = """\
    SELECT * 
    FROM sources
    WHERE nphot >= %d""" % (args.n_min_phot)

    if (args.n_max_phot > args.n_min_phot and args.n_max_phot > 0):
        sql += " AND nphot <= %d" % (args.n_max_phot)

    print sql
    query = curs.execute(sql)

    all_results = []
    while (True):
        results = query.fetchmany(size=1000)

        if (results is None or results == []):
            break

        all_results.extend(results)

    all_results = numpy.array(all_results)
    print all_results.shape

    with open(args.output, "w") as f:
        print >>f, "id ra dec rms_ra rms_dec nphot"
        numpy.savetxt(f, all_results)

    # numpy.savetxt(args.output, all_results)