'''
Created on 13 Jan 2023

@author: thomasgumbricht
'''

from glob import glob

from os.path import join

from os import walk, listdir

from pathlib import Path

import csv

def Glob_list_files(FP, src_file_hdr, pattern='**/*', recursive=True):

    search_pattern = join(FP, pattern)

    fL = glob.glob(search_pattern, recursive=recursive)

    fL = [f for f in fL if f.endswith(src_file_hdr)]

    return fL

def GlobGetFileList(FP, patternL):
    '''
    '''
    
    fL = []
        
    for pattern in patternL:
                    
        fL.extend( glob(join( FP,pattern ) ) )
    
    return (fL)

def PathLibGetFileList(FP, patternL):
    '''
    '''
    
    fL = []
    
    for pattern in patternL:
            
        for path in Path(FP).rglob(pattern):
            
            fL.appendd( path.name )
            
def CsvFileList(csvFPN):
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

def Os_walk(src_FP, src_file_hdr, pattern='',pattern_not=''):
    ''' Find all files under srcFP with the ending srf_file_hdr)
    '''
    fL = []

    n = 0
    for root, dirs, files in walk(src_FP):
        
        for file in files:
            
            if file[0] == '.':
                
                #skip hidden files
                
                continue

            if file.endswith(src_file_hdr):

                if pattern:

                    if pattern in file:

                        fL.append( join(root, file) )

                        n+=1
                        
                elif pattern_not:

                    if not pattern_not in file:

                        fL.append( join(root, file) )

                        n+=1
                else:
                
                    fL.append( join(root, file) )

                n+=1 
                                           
    return fL

def List_Dir(src_FP, src_file_hdr, pattern=''):

    # Get all files in the source directory
    files = listdir(src_FP)

    if src_file_hdr:
        files = [f for f in files if f.endswith(src_file_hdr)]

    return files
