#!/usr/bin/env python

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3



if __name__ == "__main__":

    db_file = sys.argv[1]

    matching_radius = float(sys.argv[2])
    matching_radius_deg = matching_radius / 3600.


    conn = sqlite3.connect(db_file)
    curs = conn.cursor()

    while (True):

        #
        # Find un-associated source
        #
        sql = """\
        SELECT alpha_j2000, delta_j2000
        FROM PHOTOMETRY
        WHERE sourceid IS NULL"""
        query = curs.execute(sql)

        result = query.fetchone()
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
        AND delta_j2000 > %(mindec)f
        AND delta_j2000 < %(maxdec)f
        AND alpha_j2000 > %(minra)f
        AND alpha_j2000 < %(maxra)f
        """ % {
            'minra': ra-d_ra, 'maxra': ra+d_ra,
            'mindec': dec-matching_radius_deg,
            'maxdec': dec+matching_radius_deg
        }
        associated_query = curs.execute(associated_sql)
        matches = associated_query.fetchall()
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
        pos_std  = numpy.std(matches[valid][:, 0:2], axis=0) * [cos_dec, 1.0]
        # print pos_std * 3600

        #
        # create new entry in the source table
        #
        new_sourceid_sql = "INSERT INTO sources (ra, dec) VALUES (?,?)"
        curs.execute(new_sourceid_sql, (mean_pos[0], mean_pos[1]))

        get_sourceid_sql = "SELECT sourceid FROM sources WHERE ra=? and dec=?"
        query = curs.execute(get_sourceid_sql, (mean_pos[0], mean_pos[1]))
        _source_id = query.fetchone()
        source_id = _source_id[0]
        # print source_id

        print "Found %3d sources at %8.5f %+8.5f +/- %5.3f %5.3f ==> %5d" % (
            valid_photid.shape[0], mean_pos[0], mean_pos[1],
            pos_std[0]*3600, pos_std[1]*3600,
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

        # break
        conn.commit()

    conn.close()

    # sqrt((delta_j2000 - % (dec)
    # f) ** 2 + ((alpha_j2000 - % (ra)
    # f)*cos( % (dec)
    # f)) ** 2) as poserror
