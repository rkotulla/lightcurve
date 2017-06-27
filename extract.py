#!/usr/bin/env python

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3



if __name__ == "__main__":

    db_file = sys.argv[1]

    coord_select = False
    try:
        _ra = sys.argv[2]
        _dec = sys.argv[3]
        ra = [float(f) for f in _ra.split("..")]
        dec = [float(f) for f in _dec.split("..")]
        coord_select = True
    except:
        pass

    if (coord_select):
        print ra, dec

        region_select = """\
        WHERE photometry.alpha_j2000 >= %f
         AND photometry.alpha_j2000 <= %f
         AND photometry.delta_j2000 >= %f
         AND photometry.delta_j2000 <= %f
        """ % (ra[0], ra[1], dec[0], dec[1])


    sql = """\
SELECT *
FROM photometry
JOIN frames
ON frames.frameid = photometry.frameid
%s LIMIT 10
""" % (region_select)
    print sql

    conn = sqlite3.connect(db_file)
    curs = conn.cursor()
    query = curs.execute(sql)
    print query
    results = query.fetchall()

    print results
