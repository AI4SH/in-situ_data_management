'''
Created on 5 Jan 2025
Update on 11 Mar 2025
Update on 21 Aug 2025

@author: thomasgumbricht
'''

# Standard library imports
from os import path

# Package application imports
from src.lib import Parameters_fix, Data_read, Loop_data_records

def Manage_process(json_job_D):
    """
    Manages the processing of JSON defined jobs by iterating through the job dictionary,
    extracting relevant information, and initiating the appropriate processing functions.

    @param json_job_D Dictionary containing JSON job definitions and their associated processes.
    @return None
    """
    for key in json_job_D:

        json_file_name = path.split(key)[1]

        print ('\n########### STARTING JOB ########### \n')

        msg = 'Command file: \n  %s (%s ready processes to run)' %(json_file_name, len(json_job_D[key]))

        print (msg)

        for p_nr in json_job_D[key]:   
             
            sub_process_id = json_job_D[key][p_nr].process_S.process.sub_process_id

            if json_job_D[key][p_nr].process_S.process.overwrite:

                msg = '\n    Running process nr: %s %s (overwriting)' %(p_nr, 
                    sub_process_id)

            else:

                msg = '\n    Running process nr: %s %s' %(p_nr,
                    sub_process_id)
                    
            print (msg)

            # Redirect process parameters to the corresponding package
            if sub_process_id == 'import_csv_single-lines':

                Import_csv_single_line(json_job_D[key][p_nr].process_S.process)

            else:
                
                error_msg = '❌ WARNING sub_process_id <%s> not available in import_csv_data_process.py\n \
                    (file: %s;  process nr %s)' %(sub_process_id,
                                                json_file_name,
                                                p_nr)

                print (error_msg)

                return
            
def Import_csv_single_line(process):
    """
    @brief Imports and processes a CSV data file with one record per line for AI4SH.

    This function reads method and data CSV files, validates their contents,
    extracts relevant parameters, and calls a function to process each data record.

    @param process An object containing parameters and file paths for method and data CSV files.
    @return None. Prints error messages if files or parameters are invalid.
    """

    # Check and read the method csv file
    data_pack = Parameters_fix(process.parameters.method_src_FPN)
 
    if not data_pack:

        print('❌ Error in parameter:',process.parameters.method_src_FPN)

        return None

    # Disentangle the data pack into its components
    parameter_D, unit_D, method_D, equipment_D = data_pack

    # Check and read the data csv file
    data_pack = Data_read(process.parameters.data_src_FPN)

    if not data_pack:

        print('❌ Error in data:',process.parameters.data_src_FPN)

        return None
    # Disentangle the data pack into columns (header row) and data
    column_L, data_L_L = data_pack

    # Loop all rows in the csv file
    Loop_data_records(process, column_L, data_L_L, parameter_D, unit_D, method_D, equipment_D) 