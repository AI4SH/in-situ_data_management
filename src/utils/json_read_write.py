'''
Created on 4 Jan 2024

@author: thomasgumbricht
'''

# Standard library imports



from os import path

import json

def Read_json(FPN,verbose=0):
    '''
    '''
        
    if verbose:
        
        print ('    Reading json file: %s' %(FPN)) 

    if not path.exists(FPN):
        
        msg = 'WARNING - json file not found: %s' %(FPN)

        print (msg)
        
        return None

    # Opening JSON file 
    f = open(FPN,) 

    # returns JSON object
    try:
        
        return json.load(f)
    
    except:
            
        msg = 'Error reading json file: %s' %(FPN)
            
        return None
    
def Dump_json(FPN, data, indent=2, verbose=0):
    '''
    '''
    
    if verbose:
        
        print ('    Writing json file:\n     %s' %(FPN)) 

    with open(FPN, 'w') as outfile:
        
        json.dump(data, outfile, indent=indent)
        
    return