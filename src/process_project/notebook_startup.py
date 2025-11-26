'''
Created on 21 May 2025
Last undated 7 juni 2025
Updated on 1 Sept 2025 (doxygen comments added)

@author: thomasgumbricht
'''

# Standard library imports
from os import path
 
# Package imports
from src.utils import Pprint_parameter, Read_json, Get_project_path

def Clean_pilot_list(user_json_process_L, project_FP, project_D):
    """
    @brief Cleans a list of JSON process files by removing comments and whitespace.

    This function processes a list of JSON file paths, filtering out any entries that are comments
    (lines starting with '#') or are too short to be valid file names. It returns a cleaned list
    of valid JSON file paths.

    @param user_json_process_L (list): List of JSON file paths, potentially containing comments and whitespace.
    @return list: A cleaned list of valid JSON file paths.
    """

    json_path = path.join(project_FP, project_D["process"]["job_folder"],project_D["process"]["process_sub_folder"])
    
    if not path.exists(json_path):

        print('    ❌ ERROR the path to the json process file(s) does not exist:', json_path)

        return None
    
    # Clean the list of json objects from comments and white space etc
    cleaned_list = [x.strip() for x in user_json_process_L if len(x.strip()) > 5 and x.strip()[0] != '#']

    # Clean the list of json objects from comments and whithe space etc
    cleaned_L = [path.join(json_path,x.strip())  for x in user_json_process_L if len(x) > 5 and x[0] != '#']

    return cleaned_L

def Notebook_initiate(user_project_file, project_file):

    # Check if user_project_file is a path with '~' and expand it
    # This is useful for user project files that are stored in the home directory.
    # TGTODO move the check of the user_project_file to utils
    if user_project_file[0] == '~':

        user_project_file = path.expanduser(user_project_file)

    if not path.exists(user_project_file):

        print('  ❌ ERROR the user project file does not exist:', user_project_file)

        return None
        
    user_default_params_D = Read_json(user_project_file)

    if not user_default_params_D:

        print('  ❌ ERROR the user project file is empty or not a valid JSON:', user_project_file)

        return None

    verbose = user_default_params_D['process'][0]['verbose']
            
    if verbose:
            
        Pprint_parameter(user_default_params_D)

    project_FP = Get_project_path('notebook_FP',user_default_params_D['project_path'])

    if not project_FP:

        print('  ❌ ERROR the <project_path> (%s) defined in the user project file does not exist:\n.   %s' %(user_default_params_D['project_path'], user_project_file))

        return None

    project_file_FPN = path.join(project_FP,project_file)

    if not path.exists(project_file_FPN):

        print('  ❌ ERROR the project file does not exist:\n    %s' %(project_file_FPN))
       
        return None

    # Read the projects file to get the job/process parameters.
    project_D = Read_json(project_file_FPN)

    if not project_D:

        print('  ❌ ERROR the project file is empty or not a valid JSON:', project_file_FPN)

        return None

    if verbose > 1:

        print ('====== Parameters from project file:')

        Pprint_parameter( project_D )

        print ('======')      

    # look for 1) <pilot_list>, 2) <pilot_file> or 3) <process_file> in that order in the project file
    if "pilot_list" in project_D["process"]:

        if verbose:

            print ('  Reading process files to run from array <pilot_list>')

        if not isinstance(project_D["process"]["pilot_list"], list):

            print('    ❌ ERROR the object <pilot_list> in the project file should be a list of json files')

            print('    project file with error: %s' %(project_file_FPN))
            
            return None

        process_L = project_D["process"]["pilot_list"]

        user_json_process_L = Clean_pilot_list(process_L, project_FP, project_D) 

    elif "pilot_file" in project_D["process"]:

        if verbose:

            print ('  Reading process files to run from <pilot_file>: %s' %(project_D["process"]["pilot_file"]))

        pilot_FPN = path.join(project_FP, project_D["process"]["job_folder"], project_D["process"]["pilot_file"])

        if not path.exists(pilot_FPN):

            print('    ❌ ERROR the pilot file does not exist:', pilot_FPN)

            print('    project file with error: %s' %(project_file_FPN))

            return

        # Open and read the pilot text file linking to all json files defining the project
        with open(pilot_FPN) as f:

            process_L = f.readlines()

        user_json_process_L = Clean_pilot_list(process_L, project_FP, project_D) 

    # If the object <sub_process_id>, this is not a project file, insted the user_project file points directly to a process file
    # add that single process file as a list
    elif isinstance(project_D["process"], list) and'sub_process_id' in project_D['process'][0]:

        process_file = project_file_FPN

        # TGTODO fix a utlis script
        if process_file[0] == '~':

            process_FPN = path.expanduser(process_file)

        else:

            process_FPN = path.join(project_FP, process_file)

            if not path.exists(process_FPN):

                print('    ❌ ERROR the process file does not exist:\n    ❌ %s' % process_FPN)

                return None
        
            user_json_process_L  = [process_FPN]
            
    else:

        print('    ❌ ERROR the user project file must contain one the objects <pilot_list>, <pilot_file> or <process_file>:\n    ❌ %s' % project_file_FPN)

        return None                
    
    if verbose > 1:

        print ('  Json process files:')
                
        for json_file in user_json_process_L:
                
            print ('    ',json_file)

    return user_default_params_D, user_json_process_L