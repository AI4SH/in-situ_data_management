'''
Created on 21 Aug 2025
Update on 26 Aug 2025
Updated on 1 Sept 2025 (Structure_processes replaced older call, doxygen comments added)

@author: thomasgumbricht
'''

# Package application imports
from src.process_project import Notebook_initiate, Structure_processes

from .manage_data_process import Manage_process

from src.utils import Full_path_locate

def Initiate_process(user_project_file, process_file):
    """
    Initiate the process by loading the user project file and the process file.

    Parameters:
    - user_project_file: Path to the user project file.
    - process_file: Name of the process file (root path set in user_project_file).

    Returns:
    Nothing. The function will print the status of the process.
    If the process file does not exist, it will print an error message.
    If the process file exists, it will load the user default parameters and process files,
    run the job processes loop, and manage the process.
    If the process is completed, it will print 'Done'.
    """

    print ('Initiating process with user project file:', user_project_file)

    user_project_file = Full_path_locate('None',user_project_file)

    # Load user project and process files
    success = Notebook_initiate(user_project_file, process_file)

    if not success:

        return None

    user_default_params_D, process_file_FPN_L = success

    # The process_file_FPN_L is a list of full paths to the process files to run with this job
    if process_file_FPN_L:

        # Check and structure the process files
        json_job_D = Structure_processes(user_default_params_D, process_file_FPN_L)

        # Run the structured processes
        Manage_process(user_default_params_D['project_path'],json_job_D)

        print ('Done')

    else:

        print('No process to run')