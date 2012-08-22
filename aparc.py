__author__ = 'nolan'

import numpy as np
import os
import json

def read_fs_stats(subjid, stat_type, hemi=None):
    """
    Read a FreeSurfer stat file and return a dictionary where:
        key = 'volume' or 'surface'
        value = numpy array of the data table

    Example:

    >>> stats = read_fs_stats('bert', 'aparc', hemi='rh')
    """
    try:
        SUBJECTS_DIR = os.environ['SUBJECTS_DIR']
        anatomy_type = ''
        if hemi:
            stats_file = os.path.join(SUBJECTS_DIR, subjid, 'stats', hemi + '.' + stat_type + '.stats')
        else:
            stats_file = os.path.join(SUBJECTS_DIR, subjid, 'stats', stat_type + '.stats')
        # get the anatomy_type ['volume','surface']
        with file(stats_file) as stats:
            for num, line in enumerate(stats, 1):
                if 'anatomy_type' in line:
                    anatomy_type = line.split()[-1]
        stats = np.genfromtxt(stats_file, dtype=None)
        return {anatomy_type:stats}
    except KeyError:
        print "FreeSurfer SUBJECT_DIR environment variable is not set."
        return None

def parse_stats(fs_stats):
    collection = {}

    # fields for volumetric statistics
    seg_stats = ["Index", "SegId", "NVoxels", "Volume_mm3", "StructName",
                 "normMean", "normStdDev", "normMin", "normMax", "normRange"]

    # fields for surface statistics
    surf_stats = ['StructName', 'NumVert', 'SurfArea', 'GrayVol', 'ThickAvg',
                  'ThickStd', 'MeanCurv', 'GausCurv', 'FoldInd', 'CurvInd']

    for row in aparc:
        for stat in range(len(stats)):
            entity = hemi + '.' + row[0] + '.' + stats[stat]
            collection[entity] = {'prov:type':['fs:' + hemi + '.' + row[0],'fs:' + stats[stat]],
                                  'prov:value':row[stat + 1]}
    return collection

result = parse_stats('/Applications/freesurfer/subjects/bert/stats/rh.aparc.stats', 'rh')


print result