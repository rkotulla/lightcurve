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

    columns = read_colunms_from_param_file(args.sex_param_fn)
    # columns = [p.lower() for p in params]

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

        lightcurves = {}
        for i,fn in enumerate(filters):
            print("Reading data for ID %d, filter %s" % (ids[i], fn))
            result = get_lightcurve(
                database=dbs[fn],
                sourceid=ids[i],
                sextractor_columns=columns,
                calibrate=True,
            )
            lightcurve, sqlquery, query_columns, diffphot = result
            lightcurves[fn] = lightcurve
            print(lightcurve.shape)

        #
        #  Now we have a set of lightcurves in different bands,
        #  next up we'll combine data points for similar timestamps
        #

        n_cols = 0
        columns = []
        for f in filters:
            start_at = n_cols
            n_cols += lightcurves[f].shape[1]
            end_at = n_cols
            columns.append((start_at, end_at))

        lc1 = lightcurves[filters[0]]
        combined_lightcurve = numpy.empty((lc1.shape[0], n_cols))
        combined_lightcurve[:,:] = numpy.NaN
        combined_lightcurve[:, 0:lc1.shape[1]] = lc1
        print(combined_lightcurve.shape)

        for itime, timestamp in enumerate(combined_lightcurve[:,0]):
            # print(timestamp)

            for i,f in enumerate(filters[1:]):
                d_time = numpy.fabs(lightcurves[f][:,0] - timestamp)
                # print(d_time)
                closest_time = numpy.argmin(d_time)
                # print(i, timestamp, d_time[closest_time])
                # if (d_time[closest_time] > 3600):
                #     # if time-stamps are off by more than an hour, don't consider this a valid match
                #     continue

                c1,c2 = columns[i+1]
                combined_lightcurve[itime, c1:c2] = lightcurves[f][closest_time,:]

        # save the combined lightcurve
        numpy.savetxt("combined_%s.txt" % (pairs), combined_lightcurve)

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