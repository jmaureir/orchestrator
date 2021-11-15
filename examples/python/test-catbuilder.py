#!/usr/bin/env python3

import orchestrator as orch
import argparse
import time
import pickle
import dill

from promise import Promise

def extract_ccds(mosaic_filename):
    import sys
    import tempfile
    import shutil
    sys.path.append("/home/jcm/codes/catbuilder/lib")
    import catbuilder as cb
    print("opening %s" % mosaic_filename)
    mosaic = cb.MosaicImage(mosaic_filename)
    workdir = tempfile.mkdtemp(suffix="-orch",prefix="pimcopy-",dir="/dev/shm")
    ccd_filelist = mosaic.extractAllCCDs(workdir)
    if len(ccd_filelist)>0:

        for file in glob.glob(r"%s/*.fits*" % workdir):
            shutil.copyfile(file,"./out")

        shutil.rmtree(workdir)
        return ccd_filelist
    raise RuntimeError("Could not extract CCDs from mosaic image")

def source_extractor_slow(ccd_filename):
    import sys
    sys.path.append("/home/jcm/codes/catbuilder/lib")
    import catbuilder as cb

    ccd = cb.Image(ccd_filename)
    cat = ccd.getCatalog(type="FITS_LDAC")

    return cat["ldac_catalog"]

def source_extractor(ccd_filename):
    import sys
    import shutil
    sys.path.append("/home/jcm/codes/catbuilder/lib")
    import catbuilder as cb

    ccd = cb.Image(ccd_filename)
    cat = ccd.getCatalog(type="FITS_LDAC",preserve_workdir=True)
    ldac_catalog_file = "out/%s_%s_%s_catalog.fits" % (ccd.getHeader("OBJECT"),ccd.getHeader("EXTNAME"),ccd.getHeader("MJD-OBS"))
    shutil.copyfile(cat["catalog_filename"],ldac_catalog_file)
    shutil.rmtree(cat["workdir"])

    return ldac_catalog_file

def calibrate_catalog(catalogs):
    import sys
    sys.path.append("/home/jcm/codes/catbuilder/lib")
    import catbuilder as cb
    from astropy.io import fits

    c = [fits.open(x) for x in catalogs if x is not None]

    s = cb.scamp(c, params={ "ASTREF_CATALOG": "GAIA-DR1", "MERGEDOUTCAT_NAME":"merged_catalog.cat", "MERGEDOUTCAT_TYPE":"FITS_LDAC","FULLOUTCAT_TYPE":"NONE","CHECKPLOT_DEV":"NULL"},nthreads=0)

    res = s.run().get()

    cat_list = []
    for filename in res["data"]:
        ldac_catalog = res["data"][filename]
        ldac_catalog.writeto("out/%s" % filename)
        cat_list.append("out/%s" % filename)

    return cat_list

if __name__ == "__main__":
    print("Data orchestrator test code")

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--images', help="mosaic images", nargs="*", required=True)
    args = parser.parse_args()

    mosaic_filelist=args.images

    def extract_ccd(mosaic_image):
        job=orch.Job(params={\
            "ntasks":"1",
            "job-name":"splitting",
            "cpus-per-task":"8",
            "nodes":"1"
        })
        ccd_list = job.run("extract_ccds",mosaic)
        return(job.join())

    def build_catalog_from_list(ccd_list):
        catalogs=[]
        jobs=[]
        for ccd in ccd_list:
            job=orch.Job(params={\
                "ntasks":"1",
                "job-name":"catbuilding",
                "cpus-per-task":"4",
            })
            jobs.append(job)
            cat = job.run("source_extractor",ccd)
            catalogs.append(cat)

        for job in jobs:
            job.join()

        x = [cat.get() for cat in catalogs]

        return(x)

    def build_calibrated_catalogs(mosaic_catalogs):
        print("************** BUILD CALIBRATED CATALOGS ********************** ")
        job=orch.Job(params={\
            "ntasks":"1",
            "job-name":"calibration",
            "cpus-per-task":"8",
        })
        cats = job.run("calibrate_catalog",mosaic_catalogs)
        return(job.join())

    def test_serialize():
        from orchestrator.slurm import AVROCodec
        from astropy.io.fits import HDUList

        cat_list = build_catalog_from_list(['out/Blind15A_03_N1_57072.03539183_image.fits.fz','out/Blind15A_03_S1_57072.03539183_image.fits.fz'])
        #cat_list = [ source_extractor('out/Blind15A_03_N1_57072.03539183_image.fits.fz') ]

        a = AVROCodec.encode(cat_list)
        b = AVROCodec.decode(a)

        print(b)
        cc = build_calibrated_catalogs(b)
        print(cc)


########### MAIN ###############

    #test_serialize()
    #exit(1)

    catalogs=[]

    for mosaic in mosaic_filelist:
        @orch.Async
        def process_mosaic(mosaic):
            return Promise.resolve(extract_ccd(mosaic)).then(build_catalog_from_list).then(build_calibrated_catalogs)
            #return Promise.resolve(extract_ccd(mosaic)).then(build_catalog_from_list)


        print("processing %s" % mosaic)
        cats = Promise.resolve(process_mosaic(mosaic))
        catalogs.append(cats);

    print("waiting catalogs to be done") 
    for cat_list in catalogs:
        c = cat_list.get().wait().get()
        print(c)

    print("pipeline done")
