#!/usr/bin/env python3

import os
import sys
import astropy.io.fits as pyfits
import numpy

import subprocess
import sqlite3

import argparse

from get_lightcurve import get_lightcurve, read_colunms_from_param_file


if __name__ == "__main__":

    #
    # Handle all command line stuff
    #
    parser = argparse.ArgumentParser(
        description='Extract catalog of associated sources')
    # parser.add_argument(
    #     'database_fn', type=str, nargs=2,
    #     metavar='data.base',
    #     help='database filename band #1')
    parser.add_argument("--g", dest="db_g", help="database g-band", default=None, type=str)
    parser.add_argument("--r", dest="db_r", help="database r-band", default=None, type=str)
    parser.add_argument("--i", dest="db_i", help="database i-band", default=None, type=str)

    parser.add_argument('--out', dest='output', type=str,
                        default=sys.stdout, help='output filename')
    parser.add_argument(
        'sex_param_fn', metavar='sex.param', type=str, #nargs=1,
        help='SExtractor parameter filename')

    parser.add_argument("ids", type=str,
                        help="Pair of IDs, comma-separated", nargs='+')
    args = parser.parse_args()


    # print(args.database_fn)

    # open all databases
    filters = []
    db_files = {}
    if (args.db_g is not None):
        filters.append('g')
        db_files['g'] = args.db_g
    if (args.db_r is not None):
        filters.append('r')
        db_files['r'] = args.db_r
    if (args.db_i is not None):
        filters.append('i')
        db_files['i'] = args.db_i

    params = read_colunms_from_param_file(args.sex_param_fn)
    columns = [p.lower() for p in params]

    dbs = {}
    curs = {}
    for filtername in filters:
        print(filtername, db_files[filtername])
        db = sqlite3.connect(db_files[filtername])
        dbs[filtername] = db
        curs[filtername] = db.cursor()

    lightcurves = []

    for pairs in args.ids:
        ids = [int(p) for p in pairs.split(",")]
        # print(p1,p2)

        for i,fn in enumerate(filters):
            print("Reading data for ID %d, filter %s" % (ids[i], fn))
            result = get_lightcurve(
                database=dbs[fn],
                sourceid=ids[i],
                sextractor_columns=columns,
                calibrate=True,
            )

    print("all done!")



    # #
    # # open database
    # #
    # db = sqlite3.connect(args.database_fn)
    # curs = db.cursor()
    #
    # sql = """\
    # SELECT *
    # FROM sources
    # WHERE nphot >= %d""" % (args.n_min_phot)
    #
    # if (args.n_max_phot > args.n_min_phot and args.n_max_phot > 0):
    #     sql += " AND nphot <= %d" % (args.n_max_phot)
    #
    # print(sql)
    # query = curs.execute(sql)
    #
    # all_results = []
    # while (True):
    #     results = query.fetchmany(size=1000)
    #
    #     if (results is None or results == []):
    #         break
    #
    #     all_results.extend(results)
    #
    # all_results = numpy.array(all_results)
    # print(all_results.shape)
    #
    # with open(args.output, "w") as f:
    #     print("id ra dec rms_ra rms_dec nphot", file=f)
    #     numpy.savetxt(f, all_results)
    #
    # # numpy.savetxt(args.output, all_results)