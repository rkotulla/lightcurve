#!/usr/bin/env python3

import os
import sys
import pyfits
import numpy

import subprocess
import sqlite3

from create_table import read_colunms_from_param_file

import argparse


def get_lightcurve(
        database,
        sourceid=None,
        ra=None, dec=None, match_radius=1.,
        calibrate=True,
        sextractor_columns=None,
        n_max_points = 1000,
        use_differential_photometry=False,
        diffphot_radius=1.5,
        diffphot_number=5,
):

    curs = database.cursor()


    if (sourceid is  None and
        ra is not None and dec is not None and match_radius is not None):
        #
        # First identify source id from coordinates
        #
        dec_min = dec - match_radius / 3600.
        dec_max = dec + match_radius / 3600.
        cos_dec = numpy.cos(numpy.radians(dec))
        print(cos_dec)
        ra_min = ra - match_radius / cos_dec / 3600.
        ra_max = ra + match_radius / cos_dec / 3600.
        sql = """\
        SELECT sourceid, ra, dec
        FROM sources
        WHERE 
        ra >= %f AND ra <= %f AND 
        dec >= %f AND dec <= %f
        """ % (ra_min, ra_max, dec_min, dec_max)

        # print " ".join(sql.split())
        query = curs.execute(sql)
        results = numpy.array(query.fetchmany(size=100))
        print(results)

        if (results.shape[0] <= 0):
            # no results received
            return None

        distance = numpy.hypot(
            (results[:,1]-ra) * cos_dec,
            (results[:,2] - dec)
        )
        closest = numpy.argmin(distance)
        sourceid = results[:,0][closest]

    # if (sourceid is not None):
    #     where_clause = "sources.sourceid = %d" % (sourceid)
    # elif (ra is not None and dec is not None and match_radius is not None):

    column_list_x = ['frames.mjd', 'frames.magzero', 'frames.magzero_err',
                     'frames.frameid']
    column_list_phot = ["photometry.%s" % (c.lower()) for c in sextractor_columns]
    column_list = column_list_x + column_list_phot
    # print column_list

    query_columns = ",".join(column_list)
    sql = """\
    SELECT %s
    FROM photometry
    JOIN sources ON sources.sourceid = photometry.sourceid
    JOIN frames ON frames.frameid = photometry.frameid
    WHERE sources.sourceid = %d
    ORDER by frames.mjd
    """ % (query_columns, sourceid)
    # print sql

    lc_query = curs.execute(sql)
    results = numpy.array(lc_query.fetchmany(size=n_max_points))

    if (results.shape[0] <= 0):
        return None

    if (calibrate):
        magzero = results[:,1]
        for idx, c in enumerate(sextractor_columns):
            if (c.startswith("MAG") and not c.startswith("MAGERR")):
                results[:, idx+len(column_list_x)] += magzero

    differential_photometry_correction = None
    if (use_differential_photometry):

        # find the column numbers for all magnitude columns
        mag_columns = []
        magerr_columns = []
        for i,n in enumerate(column_list):
            # print n
            if (n.startswith("photometry.mag_")):
                mag_columns.append(i)
            if (n.startswith("photometry.magerr_")):
                magerr_columns.append(i)

        #
        # get coordinates from source-id
        #
        sql = "SELECT ra,dec FROM sources WHERE sourceid = %d" % (sourceid)
        query = curs.execute(sql)
        ra_dec =  query.fetchone()
        print("#", ra_dec)
        ra, dec = ra_dec
        print("#", ra, dec)


        #
        # select nearby sources from their source-ids
        #
        dec_min, dec_max = dec-diffphot_radius/60, dec+diffphot_radius/60
        cos_dec = numpy.cos(numpy.radians(dec))
        ra_min = ra - diffphot_radius/60/cos_dec
        ra_max = ra + diffphot_radius/60/cos_dec

        sql = """SELECT sourceid,ra,dec FROM sources WHERE
              ra>%f and ra<%f and dec>%f and dec<%f""" % (
                ra_min, ra_max, dec_min, dec_max)
        # print sql
        query = curs.execute(sql)
        ref_star_candidates = numpy.array(query.fetchmany(size=5000))
        print("#", ref_star_candidates.shape)

        #
        # compute distances and decide what source-ids to use as reference
        #
        distance = numpy.hypot(
            ref_star_candidates[:,2]-dec,
            (ref_star_candidates[:,1]-ra)*cos_dec
        )
        si = numpy.argsort(distance)
        ref_star_sorted = ref_star_candidates[si]
        ref_stars = ref_star_sorted[:diffphot_number]
        print("#", ref_stars)
        ref_star_sourceids = ref_stars[:,0].astype(numpy.int)
        ref_star_sourceids = [2112] #105, 107]

        #
        # query the light-curves for these sources
        #
        all_corrections = []
        all_corrections_error = []
        source_mags = results[:, mag_columns]
        numpy.savetxt("diffphot_test.src", source_mags)
        for ref_source_id in ref_star_sourceids:
            print("# getting lightcurve for reference source %d" % (ref_source_id))
            res = get_lightcurve(
                database,
                sourceid=ref_source_id,
                ra=None, dec=None, match_radius=1.,
                calibrate=True,
                sextractor_columns=sextractor_columns,
                n_max_points = n_max_points,
                use_differential_photometry=False,
            )

            ref_lightcurve, _, _, _ = res
            numpy.savetxt("ref_lightcurve.%d" % (ref_source_id), ref_lightcurve)

            # Now match the reference lightcurve, frame-by-frame to the actual
            # lightcurve
            matched_lightcurve = numpy.empty((results.shape[0], len(mag_columns)))
            matched_lightcurve[:,:] = numpy.NaN
            matched_lightcurve_err = numpy.empty((results.shape[0], len(magerr_columns)))
            matched_lightcurve_err[:,:] = numpy.NaN

            # print "matched LC", matched_lightcurve.shape
            for iframe, frameid in enumerate(results[:,3]):
                #sys.stdout.write("%d " % (frameid))
                #sys.stdout.flush()
                # print "checking for %d in %s" % (frameid, str(ref_lightcurve[:,3]))
                _framematch = (ref_lightcurve[:,3] == frameid)
                if (numpy.sum(_framematch) != 1):
                    # either no match or too many matches, either way
                    # skip this value and move on
                    # print "couldn't find value"
                    continue

                matched_lightcurve[iframe, :] = ref_lightcurve[_framematch, mag_columns]
                matched_lightcurve_err[iframe, :] = ref_lightcurve[_framematch, magerr_columns]

            numpy.savetxt("matched_lc.%d" % (ref_source_id), matched_lightcurve)
            all_corrections.append(matched_lightcurve)
            all_corrections_error.append(matched_lightcurve_err)

                #print _framematch, frameid, ref_lightcurve[_framematch,3

        #
        # calculate the reference star correction
        #
        all_corrections = numpy.array(all_corrections)
        all_corrections_error = numpy.array(all_corrections_error)

        print("#", all_corrections.shape)

        diffphot = all_corrections - source_mags
        with open("diffphot.cat", "w") as x:
            for i in range(diffphot.shape[0]):
                numpy.savetxt(x, diffphot[i,:,:])
                print("\n"*10, file=x)
        # numpy.savetxt("diffphot.cat", diffphot[0, :, :])

        median_flux_correction = numpy.nanmedian(diffphot, axis=1)
        print("#", median_flux_correction.shape)

        diffphot -= median_flux_correction.reshape((median_flux_correction.shape[0], 1, median_flux_correction.shape[1]))
        with open("diffphot.cat2", "w") as x:
            for i in range(diffphot.shape[0]):
                numpy.savetxt(x, diffphot[i,:,:])
                print("\n"*10, file=x)
        # numpy.savetxt("diffphot.cat2", diffphot[0,:,:])
        print("#", diffphot.shape)

        # now combine all measurements
        final_correction = numpy.nanmedian(diffphot, axis=0)
        print("#", final_correction.shape)
        numpy.savetxt("diffphot.cat3", final_correction)

        all_corrections_error = numpy.hypot(all_corrections_error, 0.001)
        #all_corrections_error[~numpy.isfinite(all_corrections_error)] = 1e6
        fc2 = numpy.nansum(diffphot/all_corrections_error, axis=0) / \
            numpy.nansum(1./all_corrections_error, axis=0)
        numpy.savetxt("diffphot.catx", fc2)

        differential_photometry_correction = fc2 #final_correction


    return results, sql, column_list, differential_photometry_correction




if __name__ == "__main__":

    #
    # Handle all command line stuff
    #
    parser = argparse.ArgumentParser(
        description='Extract light-curve for a single source')
    parser.add_argument(
        'database_fn', type=str, #nargs=1,
        metavar='data.base',
        help='database filename')
    parser.add_argument(
        'sex_param_fn', metavar='sex.param', type=str, #nargs=1,
        help='SExtractor parameter filename')
    parser.add_argument('--id', dest='sourceid', type=int,
                        default=None, help='source-id')
    parser.add_argument('--ra', dest='ra', type=float,
                        default=None, help='Right ascension of source')
    parser.add_argument('--dec', dest='dec', type=float,
                        default=None, help='Declination')
    parser.add_argument('--rad', dest='match_radius', type=float,
                        default=1., help='search radius in arcsec')
    parser.add_argument('--nmax', dest='n_max_points', type=int,
                        default=1000, help='maximum number of observations')
    parser.add_argument('--out', dest='output', type=str,
                        default=None, help='output file')

    parser.add_argument('--diffphot', dest='use_diff_phot',
                        default=False, action='store_true',
                        help='use differential photometry')
    parser.add_argument('--diffr', dest='diff_phot_radius',
                        type=float, default=1.,
                        help='radius (in arcmin) to select '
                             'differential photometry reference stars')
    parser.add_argument('--diffn', dest='diff_phot_n', type=int,
                        default=5, help='number of reference stars for '
                                        'differential photometry')

    args = parser.parse_args()


    columns = read_colunms_from_param_file(args.sex_param_fn)
    # print columns

    #
    # open database
    #
    db = sqlite3.connect(args.database_fn)

    result = get_lightcurve(
        database=db,
        sourceid=args.sourceid,
        ra=args.ra, dec=args.dec, match_radius=args.match_radius,
        sextractor_columns=columns,
        calibrate=True,
        n_max_points=args.n_max_points,
        use_differential_photometry=args.use_diff_phot,
        diffphot_radius=args.diff_phot_radius,
        diffphot_number=args.diff_phot_n,
    )
    if (result is None):
        print("nothing found")
        sys.exit(0)

    lightcurve, sqlquery, query_columns, diffphotcorr = result

    if (diffphotcorr is not None):
        lightcurve = numpy.append(lightcurve, diffphotcorr, axis=1)

    # print lightcurve

    header = [
        'Lightcurve generated by %s' % (__file__),
        ' ',
        'Command was: %s' % (" ".join(sys.argv)),
        ' '
        'SQL-query used:',
        sqlquery,
    ]
    header.extend(['Column %3d: %s' % (i+1,c) for i,c in enumerate(query_columns)])
    header_txt = "\n".join(header)+"\n"
    # print header_txt

    if (args.output is not None):
        numpy.savetxt(args.output, lightcurve, header=header_txt)
    else:
        numpy.savetxt(sys.stdout, lightcurve, header=header_txt)


