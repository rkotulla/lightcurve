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

    parser.add_argument('--noclean', dest='clean', action="store_false",
                        default=True, help='clean duplicates in frame')

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

    counter=0
    while (True):
        counter += 1

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
        SELECT alpha_j2000, delta_j2000, photid, frameid
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

        #
        # Now make sure we only have a single source per frame
        #
        valid_matches = numpy.append(
            matches[valid],
            fine_d[valid].reshape((-1,1))*3600.,
            axis=1)
        unique_frameids = set(valid_matches[:,3])
        if (len(unique_frameids) == valid_matches.shape[0] or not args.clean):
            # nothing to do, all frame-ids are unique
            pass
        else:
            # at least some of the matches are duplicate matches with two
            # counterparts from a single frame
            # print "duplicate counter: %d" % (counter)

            # numpy.savetxt("validmatches_before.%d" % (counter),
            # valid_matches)
            for u_frameid in unique_frameids:
                from_this_frame = (valid_matches[:,3] == u_frameid)
                this_frame = valid_matches[from_this_frame]
                if (numpy.sum(from_this_frame) <= 1):
                    # no duplicates here, so move on
                    continue

                # print "#valid matches before: %d" % (valid_matches.shape[0])
                # print "duplicates found: %d: %s" % (
                #     numpy.sum(from_this_frame), str(this_frame[:,4]))

                # this frame has duplicate identifications
                # find the closest match
                min_d = numpy.min(this_frame[:,4])
                # print "closest match: %f" % (min_d)

                # and mark all other solutions as invalid
                duplicate = (valid_matches[:,3] == u_frameid) & \
                            (valid_matches[:,4] > min_d)
                # print "duplicates: %s --> #=%d" % (
                #     str(this_frame[duplicate][:,4]), numpy.sum(duplicate))
                valid_matches[duplicate, 4] = numpy.NaN

            # numpy.savetxt("validmatches_between.%d" % (counter),
            # valid_matches)

            # now eliminate all matches with distance marked as invalid (set
            #  to NaN)
            n_before = valid_matches.shape[0]
            valid_matches = valid_matches[numpy.isfinite(valid_matches[:,4])]
            print "Eliminated %d duplicates: %d --> %d" % (
                n_before-valid_matches.shape[0], n_before,
                valid_matches.shape[0])

            # numpy.savetxt("validmatches_after.%d" % (counter), valid_matches)

        # print valid_photid
        mean_pos = numpy.mean(valid_matches[:, 0:2], axis=0)
        # print mean_pos
        pos_std  = numpy.std(valid_matches[:, 0:2], axis=0) * [cos_dec, 1.0] * 3600.
        # print pos_std * 3600
        valid_photid = valid_matches[:,2].astype(numpy.int)


        #
        # create new entry in the source table
        #
        times.append(time.time())
        new_sourceid_sql = """\
        INSERT
        INTO sources (ra, dec, rms_ra, rms_dec, nphot)
        VALUES (?,?,?,?,?)"""

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
