'''
Created on 13 Jan 2023

@author: thomasgumbricht
'''

from glob import glob

from os.path import join

from pathlib import Path

import csv

def Glob_get_file_list(FP, pattern_L):
    '''
    '''

    f_L = []

    for pattern in pattern_L:

        f_L.extend( glob(join( FP,pattern ) ) )

    return (f_L)

def Path_lib_get_file_list(FP, pattern_L):
    '''
    '''

    f_L = []

    for pattern in pattern_L:

        for path in Path(FP).rglob(pattern):

            f_L.appendd( path.name )

def Csv_file_list(csvFPN):
    '''
    '''

    fL = []
    with open(csvFPN, 'r') as csvfile:

        # the delimiter depends on how your CSV seperates values
        csvReader = csv.reader(csvfile)

        for row in csvReader:

            # check if row is empty
            if row[0][0] == '#' or len(row[0])<4:

                continue

            fL.append(row[0])

    return fL
