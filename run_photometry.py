#!/usr/bin/env python

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3

from create_table import read_colunms_from_param_file


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

    database_fn = sys.argv[1]
    sex_conf_fn = sys.argv[2]
    sex_param_fn = sys.argv[3]
    filelist = sys.argv[4:]

    params = read_colunms_from_param_file(sex_param_fn)
    params = [p.lower() for p in params]

    for fn in filelist:

        catalog_fn = os.path.splitext(fn)[0]+".xcat"

        # run seource extractor
        if (not os.path.isfile(catalog_fn)):
            cmd = "sex -c %s -PARAMETERS_NAME %s -CATALOG_NAME %s %s" % (
                sex_conf_fn, sex_param_fn, catalog_fn, fn
            )
            print cmd
            os.system(cmd)

        # read catalog data
        data = numpy.loadtxt(catalog_fn)

        # now ingest data into database
        conn = sqlite3.connect(database_fn)
        curs = conn.cursor()

        hdulist = pyfits.open(fn)
        hdr = hdulist[0].header
        frameid = ingest_frame(
            db_connection=conn,
            filename=fn,
            mjd=hdr['MJD-OBS'],
            dateobs=0,
            skylevel=hdr['SKYLEVEL'] if 'SKYLEVEL' in hdr else -1,
            filtername=hdr['FILTER'] if 'FILTER' in hdr else "",
            exptime=hdr['EXPMEAS'] if 'EXPMEAS' in hdr else -1,
            object=hdr['OBJECT'] if 'OBJECT' in hdr else "",
            airmass=hdr['AIRMASS'] if 'AIRMASS' in hdr else -1,
            wcs_rms=hdr['WCS_RMS'] if 'WCS_RMS' in hdr else -1,
            seeing=hdr['SEEING'] if 'SEEING' in hdr else -1,
            magzero=hdr['PHOTZP_X'] if 'PHOTZP_X' in hdr else 0,
            magzero_err=hdr['PHOTZPSD'] if 'PHOTZPSD' in hdr else -1,
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

        #sql = 'COMMIT;'
        #print commit
        #print >>sqllog, sql.strip()
        #curs.execute(sql)

        conn.commit()

        conn.close()
