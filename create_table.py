#!/usr/bin/env python

import os
import sys

import sqlite3


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
photid INT AUTO_INCREMENT NOT NULL,
%s,
PRIMARY KEY (photid)
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
id       INT AUTO_INCREMENT NOT NULL,
mjd      FLOAT NOT NULL,
dateobs  TIMESTAMP,
filename VARCHAR NOT NULL,
PRIMARY KEY (id)
);
'''
    curs.execute(sql)
    conn.commit()

    conn.close()

