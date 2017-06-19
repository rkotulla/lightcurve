#!/usr/bin/env python

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3

from create_table import read_colunms_from_param_file

import argparse


def ingest_frame(db_connection,
                 filename=None, mjd=0, dateobs=0, skylevel=-1,
                 filtername=None, exptime=0, object=None,
                 airmass=-1, wcs_rms=-1, seeing=-1,
                 magzero=0, magzero_err=-1,
                 ):

    #
    # extract data and assemble SQL insert command
    #
    sql = """
    INSERT INTO frames (
      filename,
      mjd,
      dateobs,
      skylevel,
      filter,
      exptime,
      object,
      airmass,
      wcs_rms,
      seeing,
      magzero,
      magzero_err
    ) VALUES (
      '%s',%f,'%s',%f,'%s',%f,'%s',%f,%f,%f,%f,%f
    );""" % (
        filename, mjd, dateobs, skylevel, filtername, exptime, object,
        airmass, wcs_rms, seeing, magzero, magzero_err
    )

    print sql
    curs = db_connection.cursor()
    curs.execute(sql)
    db_connection.commit()

    #
    # now get the frameid back from the database
    #
    sql = """SELECT frameid FROM frames WHERE filename=?"""
    query = curs.execute(sql, [filename])
    results = query.fetchall()
    print "FRAME ID IS:", results
    #
    # In case we got multiple results, warn the user, and return the last one
    #
    last_frameid = numpy.max(numpy.array(results))
    print "USE FRAMEID:", last_frameid
    return last_frameid


if __name__ == "__main__":

    #
    # Handle all command line stuff
    #
    parser = argparse.ArgumentParser(
        description='Run photometry and ingest results into database.')
    parser.add_argument(
        'database_fn', type=str, #nargs=1,
        metavar='data.base',
        help='database filename')
    parser.add_argument(
        'sex_conf_fn', metavar='sex.conf', type=str, #nargs=1,
        help='SExtractor configuration file')
    parser.add_argument(
        'sex_param_fn', metavar='sex.param', type=str, #nargs=1,
        help='SExtractor parameter filename')
    parser.add_argument(
        'filenames', metavar='input.fits', type=str, nargs='+',
        help='(list of) input FITS filename(s)')
    parser.add_argument('--mjd', dest='mjd',
                        default='MJD-OBS', help='FITS keyword for MJD')
    parser.add_argument('--skylevel', dest='skylevel',
                        default='SKYLEVEL', help='FITS keyword for background level')
    parser.add_argument('--filter', dest='filter',
                        default='FILTER', help='FITS keyword for bandpass filter')
    parser.add_argument('--exptime', dest='exptime',
                        default='EXPTIME', help='FITS keyword for exposure time')
    parser.add_argument('--object', dest='object',
                        default='OBJECT', help='FITS keyword for object name')
    parser.add_argument('--aimass', dest='airmass',
                        default='AIRMASS', help='FITS keyword for airmass')
    parser.add_argument('--wcs_rms', dest='wcs_rms',
                        default='WCS_RMS', help='FITS keyword for WCS calibration uncertainty')
    parser.add_argument('--seeing', dest='seeing',
                        default='SEEING', help='FITS keyword for seeing')
    parser.add_argument('--magzero', dest='magzero',
                        default='PHOTZP_X', help='FITS keyword for photometric zeropoint')
    parser.add_argument('--errmagzero', dest='errmagzero',
                        default='PHOTZPSD', help='FITS keyword for photometric zeropoint uncertainty')
    args = parser.parse_args()

    #
    # Read & parse the Sextractor parameter file so we know what datafields
    # to populate in the database
    #
    params = read_colunms_from_param_file(args.sex_param_fn)
    params = [p.lower() for p in params]


    #
    # Now run all Sextractors and ingest results
    #
    for fn in args.filenames:

        catalog_fn = os.path.splitext(fn)[0]+".xcat"

        # run seource extractor
        if (not os.path.isfile(catalog_fn)):
            cmd = "sex -c %s -PARAMETERS_NAME %s -CATALOG_NAME %s %s" % (
                args.sex_conf_fn, args.sex_param_fn, catalog_fn, fn
            )
            print cmd
            os.system(cmd)

        # read catalog data
        data = numpy.loadtxt(catalog_fn)

        # now ingest data into database
        conn = sqlite3.connect(args.database_fn)
        curs = conn.cursor()

        hdulist = pyfits.open(fn)
        hdr = hdulist[0].header
        frameid = ingest_frame(
            db_connection=conn,
            filename=fn,
            mjd=hdr[args.mjd] if args.mjd in hdr else -1,
            dateobs=0,
            skylevel=hdr[args.skylevel] if args.skylevel in hdr else -1,
            filtername=hdr[args.filter] if args.filter in hdr else "",
            exptime=hdr[args.exptime] if args.exptime in hdr else -1,
            object=hdr[args.object] if args.object in hdr else "",
            airmass=hdr[args.airmass] if args.airmass in hdr else -1,
            wcs_rms=hdr[args.wcs_rms] if args.wcs_rms in hdr else -1,
            seeing=hdr[args.seeing] if args.seeing in hdr else -1,
            magzero=hdr[args.magzero] if args.magzero in hdr else 0,
            magzero_err=hdr[args.errmagzero] if args.errmagzero in hdr else -1,
        )

        #
        # Add frame to list of frames
        # TODO: Add handling of repeat ingestions of the same frame
        #


        #
        # Now ingest all source photometry
        #
        sqllog = open(fn[:-5]+".sql", "w")
        print "begin transaction"
        #sql = 'BEGIN TRANSACTION;'
        #curs.execute(sql)
        #print >>sqllog, sql

        for source in data:
            sql = """
            INSERT INTO photometry (frameid,%s) VALUES (%d,%s);
            """ % (
                ",".join(params),
                frameid,
                ",".join(["%f" % x for x in source])
            )
            curs.execute(sql.strip())
            #print sql
            print >>sqllog, sql.strip()

        conn.commit()

        conn.close()
