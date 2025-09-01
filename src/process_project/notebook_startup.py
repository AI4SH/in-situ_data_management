'''
Created on 21 May 2025
Last undated 7 juni 2025

@author: thomasgumbricht

'''

# Standard library imports
from os import path
 
# Package imports
from src.utils import Pprint_parameter, Read_json, Get_project_path

def Notebook_initiate(user_project_file, process_file):

    process_file_FPN = None

    # Check if user_project_file is a path with '~' and expand it
    # This is useful for user project files that are stored in the home directory.
    if user_project_file[0] == '~':

        user_project_file = path.expanduser(user_project_file)

    if not path.exists(user_project_file):

        print('The user project file does not exist:', user_project_file)

        process_file_FPN = None
        
    user_default_params_D = Read_json(user_project_file)

    verbose = user_default_params_D['process'][0]['verbose']
            
    if verbose:
            
        Pprint_parameter(user_default_params_D)

    project_FP = Get_project_path('notebook_FP',user_default_params_D['project_path'])

    if project_FP:

        # project_FP is derived from the user_project_file.
        # user_project_file and process_file are defined in Python cell 1 (Quick start).
        process_file_FPN = path.join(project_FP,process_file)

        if path.exists(process_file_FPN):

            if verbose:
                
                print ('process file:\n    ',process_file_FPN, '\n')
                
            if verbose > 1:

                process_D = Read_json(process_file_FPN)
                        
                Pprint_parameter( process_D ) 

        else:

            print('The process file does not exist:', process_file_FPN)

            return None, None

    else:

        print('The project path does not exist:', user_default_params_D['project_path'])

        return None, None

    if process_file_FPN:

        # Read the process file to get the process parameters.
        
        process_D = Read_json(process_file_FPN)

        if not process_D:

            print('\n❌ The process file is empty or not a valid JSON:', process_file_FPN)

            return None, None

        if "job_folder" in process_D["process"]:

            # If the process file contains a job_folder, expand and read the pilot_file

            pilot_FPN = path.join(project_FP, process_D["process"]["job_folder"], process_D["process"]["pilot_file"])

            json_path = path.join(project_FP, process_D["process"]["job_folder"],process_D["process"]["process_sub_folder"])

            if path.exists(pilot_FPN):

                if verbose:
                    
                    print ('pilot file:\n    ',pilot_FPN, '\n')

            else:

                print('The pilot file does not exist:', pilot_FPN)

                pilot_FPN = None

            # Open and read the pilottext file linking to all json files defining the project
            with open(pilot_FPN) as f:

                user_json_process_L = f.readlines()

            # Clean the list of json objects from comments and whithe space etc   
            user_json_process_L = [path.join(json_path,x.strip())  for x in user_json_process_L if len(x) > 5 and x[0] != '#']
               
        else:

            user_json_process_L = [path.join(project_FP, process_file_FPN)]
    
    if verbose > 1:

        print ('Json process files:')
                
        for json_file in user_json_process_L:
                
            print ('    ',json_file)

    return user_default_params_D, user_json_process_L