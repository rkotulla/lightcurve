#!/usr/bin/env python

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3
import time
import argparse


if __name__ == "__main__":

    #
    # Handle all command line stuff
    #
    parser = argparse.ArgumentParser(
        description='Create light-curve database for a given SExtractor configuration.')
    parser.add_argument(
        'database_fn', type=str, #nargs=1,
        metavar='data.base',
        help='database filename')
    parser.add_argument(
        'matching_radius', metavar='matching.radius', type=float,
        help='Matching radius in arcsec')
    args = parser.parse_args()

    matching_radius_deg = args.matching_radius / 3600.


    conn = sqlite3.connect(args.database_fn)
    curs = conn.cursor()
    #curs.arraysize = 250

    # create index
    print "creating index"
    sql = "CREATE INDEX coords ON photometry (alpha_j2000, delta_j2000)"
    try:
        curs.execute(sql)
    except sqlite3.OperationalError:
        pass
    print "done with index"

    while (True):

        #
        # Find un-associated source
        #
        times = []

        times.append(time.time())
        sql = """\
        SELECT alpha_j2000, delta_j2000
        FROM PHOTOMETRY
        WHERE sourceid IS NULL
        LIMIT 1"""
        query = curs.execute(sql)

        result = query.fetchone()
        times.append(time.time())
        # print result
        ra,dec = result
        # print ra,dec

        #
        # Now query all sources around this coordinate within the matching radius
        #
        cos_dec = numpy.cos(numpy.radians(dec))
        d_ra = matching_radius_deg / cos_dec
        associated_sql = """\
        SELECT alpha_j2000, delta_j2000, photid
        FROM photometry
        WHERE sourceid IS NULL
        AND alpha_j2000 BETWEEN %(minra)f AND %(maxra)f
        AND delta_j2000 BETWEEN %(mindec)f AND %(maxdec)f
        """ % {
            'minra': ra-d_ra, 'maxra': ra+d_ra,
            'mindec': dec-matching_radius_deg,
            'maxdec': dec+matching_radius_deg
        }
            #sourceid IS NULL       AND
        #        AND alpha_j2000 BETWEEN %(minra)f AND %(maxra)f
        associated_query = curs.execute(associated_sql)
        times.append(time.time())
        matches = associated_query.fetchmany(1000) #all()
        times.append(time.time())
        # print matches

        matches = numpy.array(matches)

        # print ra, dec, cos_dec
        # print
        fine_d = numpy.hypot((matches[:,0]-ra)*cos_dec,
                             (matches[:,1]-dec)
                             )
        valid = fine_d < matching_radius_deg
        valid_photid = matches[:,2][valid].astype(numpy.int)

        # print valid_photid
        mean_pos = numpy.mean(matches[valid][:, 0:2], axis=0)
        # print mean_pos
        pos_std  = numpy.std(matches[valid][:, 0:2], axis=0) * [cos_dec, 1.0] * 3600.
        # print pos_std * 3600

        #
        # create new entry in the source table
        #
        times.append(time.time())
        new_sourceid_sql = "INSERT INTO sources (ra, dec, rms_ra, rms_dec, nphot) VALUES (?,?,?,?,?)"
        curs.execute(new_sourceid_sql, (
            mean_pos[0], mean_pos[1], pos_std[0], pos_std[1], valid_photid.shape[0]))
        times.append(time.time())

        get_sourceid_sql = "SELECT sourceid FROM sources WHERE ra=? and dec=?"
        query = curs.execute(get_sourceid_sql, (mean_pos[0], mean_pos[1]))
        _source_id = query.fetchone()
        source_id = _source_id[0]
        # print source_id
        times.append(time.time())

        print "Found %3d sources at %8.5f %+8.5f +/- %5.3f %5.3f ==> %5d" % (
            valid_photid.shape[0], mean_pos[0], mean_pos[1],
            pos_std[0], pos_std[1],
            source_id
        )


        #
        # and add the source id to all sources
        #
        for photid in valid_photid:
            sql = """\
            UPDATE photometry
            SET sourceid = ?
            WHERE photid=?;"""
            curs.execute(sql, (source_id, photid))
        times.append(time.time())

        # break
        conn.commit()
        times.append(time.time())

        all_times = numpy.array(times)
        #numpy.savetxt(sys.stdout, numpy.diff(all_times).reshape((1,-1)), fmt="%.3f")

    conn.close()

    # sqrt((delta_j2000 - % (dec)
    # f) ** 2 + ((alpha_j2000 - % (ra)
    # f)*cos( % (dec)
    # f)) ** 2) as poserror
