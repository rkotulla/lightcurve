#!/usr/bin/env python

import os
import sys

import sqlite3



def read_colunms_from_param_file(sex_param_fn):

    #
    # Read the parameter file to get
    #
    parameters = []
    with open(sex_param_fn, "r") as pf:
        paramfile = pf.readlines()

        for _line in paramfile:
            line = _line.strip()
            if (line.startswith("#") or len(line) <= 0):
                continue

            keyname = line.split(" ")[0].split("(")[0]
            print keyname

            if (keyname in [
                'MAG_APER', 'MAGERR_APER',
                'FLUX_APER', 'FLUXERR_APER',
                'FLUX_RADIUS'
            ]):
                # this is one of the repeating entries
                n_repeat = int(line.split("(")[1].split(")")[0])
                keys2add = ["%s_%d" % (keyname, i+1) for i in range(
                    n_repeat)]
                print keys2add

            else:
                keys2add = [keyname]
            parameters.extend(keys2add)

    return parameters

if __name__ == "__main__":


    table_fn = sys.argv[1]

    sex_conf_fn = sys.argv[2]
    sex_param_fn = sys.argv[3]

    if (os.path.isfile(table_fn)):
        os.remove(table_fn)
        print("Table file (%s) already exists, either delete or choose "
              "different file" % (table_fn))
        #sys.exit(0)


    #
    # Read the parameter file to get
    #
    parameters = []
    with open(sex_param_fn, "r") as pf:
        paramfile = pf.readlines()

        for _line in paramfile:
            line = _line.strip()
            if (line.startswith("#") or len(line) <= 0):
                continue

            keyname = line.split(" ")[0].split("(")[0]
            print keyname

            if (keyname in [
                'MAG_APER', 'MAGERR_APER',
                'FLUX_APER', 'FLUXERR_APER',
                'FLUX_RADIUS'
            ]):
                # this is one of the repeating entries
                n_repeat = int(line.split("(")[1].split(")")[0])
                keys2add = ["%s_%d" % (keyname, i+1) for i in range(
                    n_repeat)]
                print keys2add

            else:
                keys2add = [keyname]
            parameters.extend(keys2add)

    # print parameters

    conn = sqlite3.connect(table_fn)
    curs = conn.cursor()

    columns_and_format = ['%s FLOAT' % (p.lower()) for p in parameters]
    columns = ",\n".join(columns_and_format)
    sql = '''
CREATE TABLE photometry (
photid INTEGER PRIMARY KEY,
frameid INTEGER NOT NULL,
sourceid INTEGER,
%s
);
''' % (columns)
    print sql
    curs.execute(sql)
    conn.commit()


    #
    # also create a table to hold what frames are already in the database
    #
    sql = '''
CREATE TABLE frames (
frameid  INTEGER PRIMARY KEY,
filename VARCHAR NOT NULL,
mjd      FLOAT NOT NULL,
dateobs  TIMESTAMP,
skylevel FLOAT,
filter   VARCHAR,
exptime  FLOAT,
object   VARCHAR,
airmass  FLOAT,
wcs_rms  FLOAT,
seeing   FLOAT,
magzero  FLOAT,
magzero_err FLOAT);
'''
    curs.execute(sql)
    conn.commit()

    #
    # create a table with unique source IDs
    #
    sql = '''
CREATE TABLE sources (
sourceid  INTEGER PRIMARY KEY,
ra        FLOAT,
dec       FLOAT
);
'''
    curs.execute(sql)
    conn.commit()


    conn.close()

