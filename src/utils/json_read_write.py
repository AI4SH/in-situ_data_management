'''
Created on 4 Jan 2024

@author: thomasgumbricht
'''

# Standard library imports



from os import path

import json

def Read_json(FPN,verbose=0):
    """
    @brief Reads a JSON file and returns its contents as a Python object.

    @param FPN Full path name of the JSON file to read.
    @param verbose If set to 1, prints status messages during execution. Default is 0.
    @return Returns the loaded JSON object if successful, None if the file is not found or an error occurs.
    """
        
    if verbose:
        
        print ('    Reading json file: %s' %(FPN)) 

    if not path.exists(FPN):
        
        msg = 'WARNING - json file not found: %s' %(FPN)

        print (msg)
        
        return None

    # Opening JSON file 
    #f = open(FPN,) 
    with open(FPN) as f:

        # returns JSON object
        try: 
            
            json_D = json.load(f)
        
        except:
                
            msg = 'Error reading json file: %s' %(FPN)
                
            return None
        
    return json_D
    
def Dump_json(FPN, data, indent=2, verbose=0):
    """
    @brief Dumps a Python object to a JSON file.

    @param FPN Full path name of the JSON file to write.
    @param data The Python object to write to the JSON file.
    @param indent Number of spaces to use for indentation in the output JSON file.
    @param verbose If set to 1, prints status messages during execution. Default is 0.
    """
    
    if verbose:
        
        print ('    Writing json file:\n     %s' %(FPN)) 

    with open(FPN, 'w') as outfile:
        
        try:

            json.dump(data, outfile, indent=indent)

        except:

            msg = '❌ Error writing json file: %s' %(FPN)
            
            print (msg)
            
            return None
        
    return True