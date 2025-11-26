'''
Created on 14 jan 2025
Last updated on 25 aug 2025 (complete rewrite to object oriented class structure)
Updated on 1 Sept 2025 (doxygen comments added)

@author: thomasgumbricht
'''

# Standard library imports
from os import path

# Package application imports
from src.utils import  Read_csv, Full_path_locate

def Parameters_fix(project_FP,method_src_FPN):
    """
    @brief Extracts parameter, unit, method, and equipment dictionaries from a method definition CSV file.

    @details
    This function reads a method definition CSV file, processes its contents, and returns four dictionaries mapping headers to their respective parameter, unit, method, and equipment values. It performs the following steps:
    - Checks if the provided file path exists.
    - Reads the CSV file and unpacks the header and data rows.
    - Normalizes header and row values to lowercase and replaces missing values with None.
    - Constructs column-wise lists for each header.
    - Creates dictionaries for parameters, units, methods, and equipment using the header as keys.

    @param method_src_FPN Absolute file path to the method definition CSV file.

    @return Tuple containing:
        - parameter_D (dict): Maps header to parameter name.
        - unit_D (dict): Maps header to unit name.
        - method_D (dict): Maps header to method name.
        - equipment_D (dict): Maps header to equipment name.
        Returns None if the file does not exist or if an error occurs during processing.

    @exception Prints error messages if the file path does not exist or if dictionary creation fails.
    """

    data_pack = Data_read(project_FP,method_src_FPN)
    
    if not data_pack:
        
        return None

    # Unpack the CSV to a header column list and a list of lists of data rows
    column_L, data_L_L = data_pack

    # Convert header column names to lowercase
    column_L = [col.strip().lower() for col in column_L]

    # Create a dictionary to hold column data
    column_D = {}

    for item in column_L:

        column_D[item] = []

    # Loop over the data rows and populate the column data dictionary
    for row in data_L_L:

        for c, col in enumerate(column_L):

            row_item = row[c].strip().lower()

            if row_item.lower() in ['na', 'none', 'n/a', 'nan', '', 'null']:

                row[c] = None

            column_D[col].append(row_item)

    # Create a parameter dictionary
    try:

        parameter_D = dict(zip(column_D['header'], column_D['parameter']))

    except:

        print("❌ Error creating parameter dictionary - check header: %s" %(column_D['header']))

    # Create a unit dictionary from the column data
    unit_D = dict(zip(column_D['header'], column_D['unit']))

    # Create a method dictionary from the column data
    method_D = dict(zip(column_D['header'], column_D['method']))

    # Create an equipment dictionary from the column data
    equipment_D = dict(zip(column_D['header'], column_D['equipment']))

    # Create an equipment_model dictionary from the column data
    if 'equipment_model' in column_D:
        
        equipment_model_D = dict(zip(column_D['header'], column_D['equipment_model']))
    else:
        equipment_model_D = dict.fromkeys(column_D['header'], 'unknown')

    if 'equipment_id' in column_D:
        
        equipment_id_D = dict(zip(column_D['header'], column_D['equipment_id']))
    else:
        equipment_id_D = dict.fromkeys(column_D['header'], 'unknown')

    return parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D

def Coordinates_fix(project_FP,point_name_position_sampledate_FPN):
    """
    @brief Extracts parameter, unit, method, and equipment dictionaries from a method definition CSV file.

    @details
    This function reads a method definition CSV file, processes its contents, and returns four dictionaries mapping headers to their respective parameter, unit, method, and equipment values. It performs the following steps:
    - Checks if the provided file path exists.
    - Reads the CSV file and unpacks the header and data rows.
    - Normalizes header and row values to lowercase and replaces missing values with None.
    - Constructs column-wise lists for each header.
    - Creates dictionaries for parameters, units, methods, and equipment using the header as keys.

    @param method_src_FPN Absolute file path to the method definition CSV file.

    @return Tuple containing:
        - parameter_D (dict): Maps header to parameter name.
        - unit_D (dict): Maps header to unit name.
        - method_D (dict): Maps header to method name.
        - equipment_D (dict): Maps header to equipment name.
        Returns None if the file does not exist or if an error occurs during processing.

    @exception Prints error messages if the file path does not exist or if dictionary creation fails.
    """

    data_pack = Data_read(project_FP,point_name_position_sampledate_FPN)
    
    if not data_pack:
        
        return None

    # Unpack the CSV to a header column list and a list of lists of data rows
    column_L, data_L_L = data_pack

    # Convert header column names to lowercase
    column_L = [col.strip().lower() for col in column_L]

    # Create a dictionary to hold column data
    coordinate_D = {}

    # Loop over the data rows and populate the column data dictionary
    for row in data_L_L:

        #for c, col in enumerate(column_L):

        #locus_date = '%s-%s_%s' %(row[0].lower(),row[1].lower(),row[2].lower()) 

        locus = row[3].lower() 

        #coordinate_D[locus_date] = {'latitude': float(row[3]),
        #                                    'longitude': float(row[4])}
        
        coordinate_D[locus] = dict(zip(column_L, row))

        coordinate_D[locus]['latitude'] = float(coordinate_D[locus]['latitude'])
        
        coordinate_D[locus]['longitude'] = float(coordinate_D[locus]['longitude'])

    return coordinate_D

def Data_read(project_FP,data_FPN):

    data_FPN = Full_path_locate(project_FP, data_FPN)
   
    if not path.exists(data_FPN):

        print ("❌ The data path does not exist: %s" % (data_FPN))

        return None

    data_pack = Read_csv(data_FPN)

    if not data_pack:
        
        return None
    
    return data_pack