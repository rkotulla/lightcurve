#!/usr/bin/env python

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3

from create_table import read_colunms_from_param_file
from get_lightcurve import get_lightcurve

import argparse




if __name__ == "__main__":

    database_file = sys.argv[1]
    sex_param_file = sys.argv[2]

    #print database_file, sex_param_file


    columns = read_colunms_from_param_file(sex_param_file)
    db = sqlite3.connect(database_file)
    print "all ready to go"

    for sourceid in range(1,20):

        result = get_lightcurve(
            database=db,
            sourceid=sourceid,
            sextractor_columns=columns,
            calibrate=True,
        )
        if (result is None):
            print "nothing found"
            continue

        lightcurve, sqlquery, query_columns = result
        #print lightcurve

        magnitudes = lightcurve[:,8]
        errors = lightcurve[:,12]
        print magnitudes


        mean_mag = numpy.mean(magnitudes)
        median_mag = numpy.median(magnitudes)
        mean_error = numpy.mean(errors)

        max_mag = numpy.max(magnitudes)
        min_mag = numpy.min(magnitudes)

        if ((max_mag-min_mag) > 3*mean_error):
            print "interesting"
        else:
            print "boring"