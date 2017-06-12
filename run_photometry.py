#!/usr/bin/env python

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3

from create_table import read_colunms_from_param_file

if __name__ == "__main__":

    sex_conf_fn = sys.argv[1]
    sex_param_fn = sys.argv[2]
    database_fn = sys.argv[3]
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
            os.system(cmd)

        # read catalog data
        data = numpy.loadtxt(catalog_fn)

        # now ingest data into database
        conn = sqlite3.connect(database_fn)
        curs = conn.cursor()

        #
        # Add frame to list of frames
        # TODO: Add handling of repeat ingestions of the same frame
        #


        #
        # Now ingest all source photometry
        #
        sqllog = open("sql.log", "w")
        print "begin transaction"
        #sql = 'BEGIN TRANSACTION;'
        #curs.execute(sql)
        #print >>sqllog, sql

        for source in data:
            sql = """
            INSERT INTO photometry (frameid,%s) VALUES (%d,%s);
            """ % (
                ",".join(params),
                1,
                ",".join(["%f" % x for x in source])
            )
            curs.execute(sql.strip())
            print sql
            print >>sqllog, sql.strip()

        #sql = 'COMMIT;'
        #print commit
        #print >>sqllog, sql.strip()
        #curs.execute(sql)

        conn.commit()

        conn.close()
