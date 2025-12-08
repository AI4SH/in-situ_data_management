'''
Created on 5 Jan 2025
Update on 11 Mar 2025
Update on 21 Aug 2025
Updated on 1 Sept 2025 (doxygen comments added)
Updated on 8 December 2025 (removed redundant imports)

@author: thomasgumbricht
'''

# Standard library imports
from os import path

# Package application imports
from src.lib import Parameters_fix, Data_read, Process_ai4sh_csv, Process_neospectra_csv, Process_ds2500_csv

from src.utils import Full_path_locate, Remove_path

def Manage_process(project_FP,json_job_D):
    """
    Manages the processing of JSON defined jobs by iterating through the job dictionary,
    extracting relevant information, and initiating the appropriate processing functions.

    @param json_job_D Dictionary containing JSON job definitions and their associated processes.
    @return None
    """
    for job in json_job_D:

        json_file_name = path.split(job)[1]

        print ('\n########### STARTING JOB ########### \n')

        msg = 'Command file: \n  %s (%s ready processes to run)' %(json_file_name, len(json_job_D[job]))

        print (msg)

        for p_nr in json_job_D[job]:  

            if not json_job_D[job][p_nr].process_S.process.translate: 

                continue
 
            sub_process_id = json_job_D[job][p_nr].process_S.process.sub_process_id

            if json_job_D[job][p_nr].process_S.process.overwrite:

                msg = '\n    Running process nr: %s %s (overwriting)' %(p_nr, 
                    sub_process_id)

                for item in ['ai4sh','xspectre']:

                    dst_FP= '%s_%s' %(json_job_D[job][p_nr].process_S.process.parameters.dst_FP,item)

                    dst_FP = Full_path_locate(project_FP, dst_FP, True)

                    Remove_path(dst_FP)

            else:

                msg = '\n    Running process nr: %s %s' %(p_nr,
                    sub_process_id)
                    
            print (msg)

            # Redirect process parameters to the corresponding package
            if sub_process_id == 'import_ai4sh_csv':

                Import_ai4sh_csv(project_FP,json_job_D[job][p_nr].process_S.process)

            elif sub_process_id == 'import_xspectre_json_v089':

                Import_xspectre_json_v089(project_FP,json_job_D[job][p_nr].process_S.process)

            elif sub_process_id == 'import_neospectra_csv':

                Import_neospectra_csv(project_FP,json_job_D[job][p_nr].process_S.process)

            elif sub_process_id == 'import_ds2500_csv':

                Import_ds2500_csv(project_FP,json_job_D[job][p_nr].process_S.process)

            else:
                
                error_msg = '❌  <%s> not available in import_csv_data_process.py\n \
                    (file: %s;  process nr %s)' %(sub_process_id,
                                                json_file_name,
                                                p_nr)

                print (error_msg)

                return
  
def Import_ai4sh_csv(project_FP,process):

    # Check and read the methodWARNING sub_process_id csv file
    data_pack = Parameters_fix(project_FP,process.parameters.method_src_FPN)
 
    if not data_pack:

        print('❌ Error in parameter:',process.parameters.method_src_FPN)

        return None

    # Disentangle the data pack into its components
    parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D = data_pack

    # Check and read the data csv file
    data_pack = Data_read(project_FP,process.parameters.data_src_FPN)

    if not data_pack:

        print('❌ Error in data:',process.parameters.data_src_FPN)

        return None
    # Disentangle the data pack into columns (header row) and data
    column_L, data_L_L = data_pack

    # Loop all rows in the csv file
    Process_ai4sh_csv(project_FP, process, column_L, data_L_L, parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D)
    
def Import_xspectre_json_v089(project_FP,process):
    """
    @brief Imports and processes xspectre JSON data v089 file for AI4SH.

    This function reads data JSON files with both data and metadata, validates their contents,
    extracts relevant parameters, and calls a function to process each data record.

    @param process An object containing parameters and file paths for method and data CSV files.
    @return None. Prints error messages if files or parameters are invalid.
    """

    #from src.utils import Os_walk
    from src.lib import Process_xspectre_json_v089

    # Check and read the method csv file
    Process_xspectre_json_v089(project_FP, process)

def Import_neospectra_csv(project_FP,process):
    """
    @brief Imports and processes NeoSpectra JSON data file for AI4SH.

    This function reads data JSON files with both data and metadata, validates their contents,
    extracts relevant parameters, and calls a function to process each data record.

    @param process An object containing parameters and file paths for method and data CSV files.
    @return None. Prints error messages if files or parameters are invalid.
    """

    # Check and read the methodWARNING sub_process_id csv file
    data_pack = Parameters_fix(project_FP,process.parameters.method_src_FPN)
 
    if not data_pack:

        print('❌ Error in parameter:',process.parameters.method_src_FPN)

        return None

    # Disentangle the data pack into its components
    parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D = data_pack

    # Check and read the data csv file
    data_pack = Data_read(project_FP,process.parameters.data_src_FPN)

    if not data_pack:

        print('❌ Error in data:',process.parameters.data_src_FPN)

        return None
    # Disentangle the data pack into columns (header row) and data
    column_L, data_L_L = data_pack

    # Loop all rows in the csv file
    Process_neospectra_csv(project_FP, process, column_L, data_L_L, parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D)

def Import_ds2500_csv(project_FP,process):

     # Check and read the methodWARNING sub_process_id csv file
    data_pack = Parameters_fix(project_FP,process.parameters.method_src_FPN)
 
    if not data_pack:

        print('❌ Error in parameter:',process.parameters.method_src_FPN)

        return None

    # Disentangle the data pack into its components
    parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D = data_pack

    # Check and read the data csv file
    data_pack = Data_read(project_FP,process.parameters.data_src_FPN)

    if not data_pack:

        print('❌ Error in data:',process.parameters.data_src_FPN)

        return None
    # Disentangle the data pack into columns (header row) and data
    column_L, data_L_L = data_pack

    # Loop all rows in the csv file
    Process_ds2500_csv(project_FP, process, column_L, data_L_L, parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D)

