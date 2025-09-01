'''
Created on 21 Aug 2025
Update on 26 Aug 2025

@author: thomasgumbricht
'''

# Package application imports
from src.process_project import Notebook_initiate, Job_processes_loop

from .import_csv_data_process import Manage_process

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

    # Load user project and process file
    user_default_params_D, process_file_FPN_L = Notebook_initiate(user_project_file, process_file)

    if process_file_FPN_L:

        json_job_D = Job_processes_loop(user_default_params_D, process_file_FPN_L)

        Manage_process(json_job_D)

        print ('Done')

    else:

        print('No process to run')
