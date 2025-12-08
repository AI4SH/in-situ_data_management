'''
notebook_startup.py

Created on 21 May 2025
Last undated 7 juni 2025
Updated on 1 September 2025 (doxygen comments added)
Updated on 8 December 2025

@author: thomasgumbricht
'''

# Standard library imports
from os import path
 
# Package imports
from src.utils import Pprint_parameter, Read_json, Get_project_path

def Clean_pilot_list(user_json_process_L, project_FP, project_D):
    """
    @brief Cleans and validates a list of JSON process file paths by removing comments and whitespace.

    @details
    This function processes a list of JSON file paths (either from a pilot file or pilot_list array),
    filtering out invalid entries and constructing full file paths. It performs the following operations:
    - Constructs the full path to the JSON process files directory
    - Validates that the directory exists
    - Filters out comments (lines starting with '#')
    - Filters out entries that are too short to be valid file names (< 5 characters)
    - Strips whitespace from valid entries
    - Constructs full file paths by joining with the JSON directory path

    @param user_json_process_L (list): List of JSON file names or paths.
    @param project_FP (str): The project root file path.
    @param project_D (dict): Project dictionary containing process configuration.

    @return list: A cleaned list of full file paths to valid JSON process files.
                 Returns None if the JSON process directory does not exist.

    @exception Prints error message and returns None if the constructed JSON path does not exist.
    """

    json_path = path.join(project_FP, project_D["process"]["job_folder"],project_D["process"]["process_sub_folder"])
    
    if not path.exists(json_path):

        print('    ❌ ERROR the path to the json process file(s) does not exist:', json_path)

        return None
    
    # Clean the list of json objects from comments and white space and too short names
    cleaned_L = [path.join(json_path,x.strip())  for x in user_json_process_L if len(x) > 5 and x[0] != '#']

    return cleaned_L

def Notebook_initiate(user_project_file, project_file):
    """
    @brief Initializes a notebook processing workflow by validating and loading project configuration files.

    @details
    This function orchestrates the startup process for AI4SH notebook-based data processing. It performs
    comprehensive validation and loading of configuration files in the following sequence:
    
    1. **User Project File Validation**:
       - Expands home directory paths (starting with '~')
       - Validates file existence
       - Reads and validates JSON structure
       - Extracts verbosity settings
    
    2. **Project Path Resolution**:
       - Resolves the project root path from user configuration
       - Validates project path existence
    
    3. **Project File Processing**:
       - Constructs full path to project file
       - Validates file existence
       - Reads and validates JSON structure
       - Optionally prints parameters based on verbosity level
    
    4. **Process File Discovery** (in order of precedence):
       - **pilot_list**: Array of JSON file names in project file
       - **pilot_file**: Text file containing list of JSON files
       - **sub_process_id**: Direct process file specification
    
    5. **Process List Cleaning**:
       - Removes comments and whitespace
       - Constructs full file paths
       - Validates directory structure

    @param user_project_file (str): Path to the user project JSON file, which may start with '~' for
                                   home directory. 
    @param project_file (str): Relative path (from project root) to the project configuration JSON file,
                              which should contain one of:
                              - process.pilot_list: Array of JSON process file names
                              - process.pilot_file: Name of text file listing JSON process files
                              - process[0].sub_process_id: Direct process specification

    @return tuple: Returns a tuple (user_default_params_D, user_json_process_L) where:
                  - user_default_params_D (dict): Dictionary of user project parameters
                  - user_json_process_L (list): List of full paths to JSON process files
                  Returns None if any validation step fails.

    @note The function supports three different ways to specify process files:
          1. Direct list in project file (pilot_list)
          2. External text file with list (pilot_file)
          3. Single process file (sub_process_id in process array)
          
          Verbosity levels control output:
          - verbose=0: Minimal output
          - verbose=1: Standard output with progress messages
          - verbose>1: Detailed output including all parameters

    @exception Prints detailed error messages for various failure conditions:
               - Missing or invalid user project file
               - Invalid project path
               - Missing or invalid project file
               - Missing pilot_list, pilot_file, or sub_process_id
               - Non-existent process files or directories
               
               Returns None for any error condition.
    """

    # Check if the user project file is in the user home directory, if so exapnd path
    if user_project_file[0] == '~':

        user_project_file = path.expanduser(user_project_file)

    # Check if the user project file exists
    if not path.exists(user_project_file):

        print('  ❌ ERROR the user project file does not exist:', user_project_file)

        return None
    
    # Read the user project file to get the project path and other default parameters.
    user_default_params_D = Read_json(user_project_file)

    if not user_default_params_D:

        print('  ❌ ERROR the user project file is empty or not a valid JSON:', user_project_file)

        return None

    # Set verbosity
    verbose = user_default_params_D['process'][0]['verbose']
            
    if verbose > 1:
            
        print ('====== Parameters from user project file:')

        Pprint_parameter( user_default_params_D )

        print ('======') 

    # Get the project root path from the user project file
    project_FP = Get_project_path('notebook_FP',user_default_params_D['project_path'])

    if not project_FP:

        print('  ❌ ERROR the <project_path> (%s) defined in the user project file does not exist:\n.   %s' %(user_default_params_D['project_path'], user_project_file))

        return None

    # Get the full path to the project file
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