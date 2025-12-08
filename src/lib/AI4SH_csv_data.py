'''
Ai4SH_4_Ai4SH.py
Created on 14 jan 2025
Last updated on 25 aug 2025 (complete rewrite to object oriented class structure)
Updated on 1 Sept 2025 (doxygen comments added)
Updated 1 October - split out to common class for xspectre and ai4sh json structures
Updated 27 Nov 2025 - cleaned code using GitHub Copilot

@author: thomasgumbricht
'''

# Standard library imports
from copy import deepcopy

# Package application imports
from src.lib import Coordinates_fix, Data_read

from .common import common_json_db

# Default variables
AI4SH_Key_L = ["pilot_site",
               "point_id",
               "depth",
               "sample_id",
               "sample_preparation__name",
               "subsample",
               "replicate",  
               "instrument_model__name",
                "instrument_id",
                "instrument_setting",
                "sample_analysis_date",
                "license",
                "doi",
                "user_analysis__email"]

SINGLE_METHOD_INSTRUMENTS = ['digit-soil-sear','slakes','soil-cylinder-drying@105c','microbiometer','single-ring-infiltration']

def Parameters_fix(project_FP,method_src_FPN):
    """
    @brief Extracts parameter, unit, method, and equipment dictionaries from a method definition CSV file.

    @details
    This function reads a method definition CSV file, processes its contents, and returns six dictionaries mapping headers to their respective parameter, unit, method, equipment, equipment_model, and equipment_id values. It performs the following steps:
    - Reads the CSV file using the Data_read function and validates the file exists.
    - Unpacks the CSV header and data rows from the returned data pack.
    - Normalizes header column names to lowercase and strips whitespace.
    - Processes each data row, normalizing values to lowercase and converting null-like values to None.
    - Creates dictionaries for parameters, units, methods, equipment, equipment_model, and equipment_id using headers as keys.
    - Provides fallback values for optional equipment_model and equipment_id columns if they don't exist.

    @param project_FP (str): The project file path used by Data_read function.
    @param method_src_FPN (str): Absolute file path to the method definition CSV file.

    @return tuple: A tuple containing six dictionaries in the following order:
        - parameter_D (dict): Maps header to parameter name.
        - unit_D (dict): Maps header to unit name.
        - method_D (dict): Maps header to method name.
        - equipment_D (dict): Maps header to equipment name.
        - equipment_model_D (dict): Maps header to equipment model name (defaults to 'unknown' if column missing).
        - equipment_id_D (dict): Maps header to equipment ID (defaults to 'unknown' if column missing).
        Returns None if the Data_read function fails or the file does not exist.

    @note The function expects the CSV file to contain at minimum the columns: 'header', 'parameter', 'unit', 'method', 'equipment'.
          The 'equipment_model' and 'equipment_id' columns are optional.

    @exception Prints error messages if parameter dictionary creation fails due to missing 'header' column.
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

class json_db(common_json_db):
    """
    @class json_db
    @brief Handles the processing and structuring of AI4SH in-situ data management records, exporting them to hierarchical JSON format.

    @details
    The json_db class inherits from common_json_db and provides specialized methods for processing AI4SH-specific 
    CSV data into structured JSON format. This class is specifically designed for the AI4SH  
    project's in-situ data management workflow.

    The class provides comprehensive functionality to:
    - Initialize with AI4SH-specific process parameters and coordinate data
    - Process CSV data containing soil measurements and observations
    - Validate parameter consistency across method, equipment, and unit dictionaries
    - Transform flat CSV data into hierarchical record structures
    - Handle special procedures like KTIMA wetlab sample naming conventions
    - Map equipment to methods and manage observation data structures
    - Extract and process numerical measurements with error handling
    - Generate unique identifiers for pilots, sites, points, and samples
    - Set comprehensive metadata for observations, samples, and sampling logs
    - Assemble complete sample event dictionaries in both AI4SH and xSpectre formats
    - Export structured data to JSON files with proper hierarchical organization
    - Support OSSL (Open Soil Spectral Library) format conversion for wetlab data

    Key processing capabilities include:
    - European decimal notation handling (comma to dot conversion)
    - Multi-format JSON export (AI4SH and xSpectre compatibility)
    - Coordinate and location data integration from separate CSV files
    - Instrument and method metadata management
    - Sample depth parameter extraction from naming conventions
    - Comprehensive data validation and error reporting

    @note This class is specifically designed for AI4SH project workflows and expects data to follow
          AI4SH naming conventions and format requirements. It requires coordinate data to be provided
          in a separate CSV file following the expected schema.

    @see common_json_db for inherited base functionality
    @see Parameters_fix() for extracting parameter mappings from CSV files
    @see Process_ai4sh_csv() for the main processing workflow using this class
    """

    def __init__(self, project_FP,process, coordinate_D):
        """
    @brief Constructor for the json_db class that initializes AI4SH-specific JSON database processing.

    @details
    This constructor initializes a json_db instance by calling the parent class constructor (common_json_db)
    with the provided parameters. The json_db class is specifically designed for processing AI4SH in-situ
    data management records and converting them to hierarchical JSON format. The constructor sets up the
    foundational attributes needed for:
    - Processing CSV data rows into structured records
    - Managing coordinate and location data
    - Handling process parameters and configuration
    - Setting up data structures for JSON export

    The constructor delegates all initialization work to the parent class common_json_db, which handles
    the common functionality shared between AI4SH and xSpectre JSON processing workflows.

    @param project_FP (str): The project file path used for file operations, data reading, and output generation.
                            This path serves as the root directory for all project-related file operations.
    @param process (object): Object containing process parameters and configuration settings, expected to have
                           a 'parameters' attribute with processing-specific settings like procedure type,
                           file paths, and analysis parameters.
    @param coordinate_D (dict): Dictionary containing coordinate and location data for sample points,
                              typically loaded from a separate CSV file containing point names, positions,
                              sample dates, and geographic coordinates.

    @return None: Constructor does not return a value but initializes the instance with inherited attributes
                 from common_json_db including process parameters, coordinate data, and base functionality.

    @note This constructor must be called with valid coordinate data already loaded. The coordinate_D
          parameter should contain all necessary location and timing information for the sample points
          being processed.

    @exception May raise exceptions if the parent class constructor fails due to invalid parameters
               or missing required data structures.

    @see common_json_db.__init__() for the actual initialization implementation.
    @see Coordinates_fix() for loading coordinate data from CSV files.
    """

        common_json_db.__init__(self, project_FP,process, coordinate_D)

    def _Distill_parameters(self, dict_D, parameter_id):
        """
        @brief Distills relevant parameters from a dictionary based on the column headers and assigns them to the appropriate attribute.

        This function checks each item in self.column_L against the provided dictionary dict_D. If the item exists and its value is not 'none',
        it is added to a distilled dictionary. If an item is missing, an error message is printed and None is returned. The distilled dictionary
        is then assigned to the corresponding attribute (parameter_D, method_D, equipment_D, unit_D, or url_D) based on the parameter_id argument.

        @param dict_D Dictionary containing parameter values to be distilled.
        @param parameter_id String specifying which attribute to assign the distilled dictionary to. Accepted values: 'parameter', 'method', 'equipment', 'unit', 'url'.

        @return True if successful, None if a required parameter is missing.
        """

        distill_D = {}

        for item in self.column_L:

            if item in dict_D:

                if dict_D[item] != 'none':

                    distill_D[item] = dict_D[item]
                    
            else:

                print(' ❌ ERROR - parameter <%s> is missing in the header dictionary' %(item))
               
                print('  Check the header definition file: %s' %(self.process_parameters.method_src_FPN))
               
                print('  and/or the data file: %s' %(self.process_parameters.data_src_FPN))
               
                return None
        
        if parameter_id == 'parameter':
            self.parameter_D = distill_D

        elif parameter_id == 'method':
            self.method_D = distill_D

        elif parameter_id == 'equipment':
            self.equipment_D = distill_D

        elif parameter_id == 'equipment_model':
            self.equipment_model_D = distill_D

        elif parameter_id == 'equipment_id':
            self.equipment_id_D = distill_D

        elif parameter_id == 'unit':
            self.unit_D = distill_D

        elif parameter_id == 'url':
            self.url_D = distill_D
        
        return True

    def _Check_parameter_consistency(self):
        """
        @brief Checks consistency between method, equipment, and unit parameter dictionaries.

        This function verifies that:
        - Every key in method_D exists in equipment_D.
        - Every key in equipment_D exists in unit_D.
        - Every key in unit_D exists in method_D.

        If any inconsistency is found, an error message is printed and False is returned.

        @return True if all dictionaries are consistent, False otherwise.
        """

        for key in self.method_D:

            if key not in self.equipment_D:

                print(f" ❌ ERROR - equipment <{key}> is missing in the parameter dictionary")
                
                return False

        for key in self.equipment_D:

            if key not in self.unit_D:
                
                print(f" ❌ ERROR - unit <{key}> is missing in the parameter dictionary")
                
                return False

        for key in self.unit_D:

            if key not in self.method_D:
                
                print(f" ❌ ERROR - method <{key}> is missing in the parameter dictionary")
                
                return False

        return True
    
    def _Convert_to_lower(self,data_row):
        """
        @brief Converts all string entries in a list to lowercase.

        This function iterates through the provided list of row data and converts each string entry to lowercase.
        Non-string entries are left unchanged.

        @param row_data List of values representing a single row of data.

        @return List with all string entries converted to lowercase.
        """

        return [item.lower() if isinstance(item, str) else item for item in data_row]

    def _Row_data_to_dict(self, row_data):
        """
    @brief Converts a row of data into a dictionary using column headers as keys and adds default parameters.

    @details
    This function takes a list of row data and maps each value to its corresponding column name
    from self.column_L, creating a dictionary where keys are column names and values are row entries.
    The function also adds a default parameter 'n_repetitions' set to 1, which indicates the number
    of repetitions for the measurement (default assumption is single measurement per sample).
    The resulting dictionary is assigned to self.record_D for further processing.

    @param row_data (list): List of values representing a single row of data from the CSV file.
                           The length should match the number of columns in self.column_L.

    @return None: The function modifies the instance by creating self.record_D dictionary.
                 Does not return a value but stores the result in the instance attribute.

    @note This function must be called after _Set_column_L() has populated self.column_L with
          the appropriate column headers. The function assumes that the length of row_data
          matches the length of self.column_L.

    @exception No explicit exception handling, but will raise an exception if row_data length
               does not match self.column_L length during the zip operation.

    @see _Set_column_L() for setting up the column headers list.
    @see _Rearrange_row_data_2_record() for further processing of the created dictionary.
    
    @warning Ensure that row_data contains the same number of elements as self.column_L
             to avoid indexing errors during dictionary creation.
    """

        self.record_D = dict(zip(self.column_L, row_data))

        self.record_D['n_repetitions'] = 1

    def _Rearrange_row_data_2_record(self):
        """
        @brief Restructures row data dictionary into a hierarchical record format for observation processing.

        @details
        This method transforms the flat row data dictionary (created by _Row_data_to_dict) into a hierarchical 
        structure suitable for AI4SH JSON format. It performs the following operations:
        - Creates nested dictionaries for observation data (value, standard_deviation, units, etc.).
        - Processes numerical values from CSV, handling decimal comma notation by converting to dot notation.
        - Converts numeric strings to float values, setting None for invalid conversions.
        - Maps observation data to standardized parameter names and units.
        - Normalizes location identifiers (pilot_country, pilot_site, sample_id, point_id).
        - Sets instrument and method metadata for each observation indicator.
        - Applies fallback values from process parameters when equipment data is unknown.

        The function creates the following hierarchical structure in self.record_D:
        - 'value': Dictionary mapping indicators to numerical measurement values
        - 'standard_deviation': Dictionary for measurement uncertainties (set to None)
        - 'unit__name': Dictionary mapping indicators to their measurement units
        - 'indicator__name': Dictionary mapping indicators to standardized parameter names
        - 'procedure': Dictionary mapping indicators to analysis procedures
        - 'analysis_method__name': Dictionary mapping indicators to analysis methods
        - 'instrument_brand__name': Dictionary mapping indicators to instrument brands
        - 'instrument_model__name': Dictionary mapping indicators to instrument models
        - 'instrument_id': Dictionary mapping indicators to instrument identifiers

        @return None: The function modifies self.record_D in place and does not return a value.

        @note This function must be called after:
            - _Row_data_to_dict() has created the initial record dictionary
            - _Distill_parameters() has populated the parameter mapping dictionaries
            - _Set_equipment_method() has created the indicator mapping structures
            
            The function handles European decimal notation (comma) by converting to standard dot notation.

        @exception Does not raise exceptions but handles ValueError and TypeError when converting strings to float,
                setting problematic values to None. Handles KeyError when accessing dictionary mappings,
                setting missing values to None.

        @see _Row_data_to_dict() for creating the initial record dictionary structure.
        @see _Distill_parameters() for setting up parameter mapping dictionaries.
        @see _Set_equipment_method() for creating indicator mapping structures.
        
        @warning This function assumes that self.indicator_L, self.indicator_D, and related mapping dictionaries
                have been properly initialized. Missing mappings will result in KeyError exceptions.
        """

        self.record_D['value'] = {}

        self.record_D['standard_deviation'] = {}

        self.record_D['unit__name'] = {}

        self.record_D['indicator__name'] = {}

        self.record_D['procedure'] = {}

        self.record_D['analysis_method__name'] = {}

        self.record_D['instrument_brand__name'] = {}

        self.record_D['instrument_model__name'] = {}; 

        self.record_D['instrument_id'] = {}

        for ind in self.indicator_L:

            if ',' in self.record_D[self.indicator_D[ind]]:
                                    
                self.record_D[self.indicator_D[ind] ] = self.record_D[self.indicator_D[ind]].replace(',','.')
                          
            try:

                self.record_D['value'][ind] = float(self.record_D[self.indicator_D[ind]])

            except (ValueError, TypeError):

                self.record_D['value'][ind] = None

            self.record_D['standard_deviation'][ind] = None

            self.record_D['unit__name'][ind] = self.unit_D[self.indicator_D[ind]]

            self.record_D['indicator__name'][ind] = self.parameter_D[self.indicator_D[ind]]

            if 'pilot_country' in self.record_D:

                # Convert pilot_country, pilot_site and sample_id to lower case and replace spaces with '-'
                self.record_D['pilot_country'] = self.record_D['pilot_country'].lower().replace(' ','-')

            if 'pilot_site' in self.record_D:
                
                self.record_D['pilot_site'] = self.record_D['pilot_site'].lower().replace(' ','-')
            
            if not 'point_id' in self.record_D:
                
                self.record_D['point_id'] = self.record_D['sample_id'].lower().replace(' ','-')

            for item in ['procedure','analysis_method__name','instrument_brand__name','instrument_model__name','instrument_id']:
                
                if self.process_parameters.procedure in SINGLE_METHOD_INSTRUMENTS:
                    try:

                        if item == 'procedure':

                            self.record_D[item][ind] = self.process_parameters.procedure

                        if item == 'analysis_method__name':

                            self.record_D[item][ind] = self.process_parameters.procedure
                    
                        elif item == 'instrument_brand__name':

                            self.record_D[item][ind] = self.process_parameters.instrument_brand__name
                    
                        elif item == 'instrument_model__name':

                            self.record_D[item][ind] = self.process_parameters.instrument_model__name
                        
                        elif item == 'instrument_id':

                            self.record_D[item][ind] = self.process_parameters.instrument_id

                    except KeyError:

                        self.record_D[item][ind] = None

                    if self.record_D[item][ind].lower() in ['unknown', 'null']:

                        if item in self.process_parameters_D:
                                
                            self.record_D[item][ind] = self.process_parameters_D[item]

                else:

                    try:

                        if item == 'procedure':

                            self.record_D[item][ind] = self.process_parameters.procedure

                        if item == 'analysis_method__name':

                            self.record_D[item][ind] = '%s-%s' %(self.method_D[self.indicator_D[ind]],ind)
                    
                        elif item == 'instrument_brand__name':

                            self.record_D[item][ind] = '%s-%s' %(self.method_D[self.indicator_D[ind]],ind)
                    
                        elif item == 'instrument_model__name':

                            self.record_D[item][ind] = self.equipment_model_D[self.indicator_D[ind]]
                        
                        elif item == 'instrument_id':

                            self.record_D[item][ind] = self.equipment_id_D[self.indicator_D[ind]]

                    except KeyError:

                        self.record_D[item][ind] = None

                    if self.record_D[item][ind].lower() in ['unknown', 'null']:

                        if item in self.process_parameters_D:
                                
                            self.record_D[item][ind] = self.process_parameters_D[item]

    def _Sample_name_parameters_ktima_wetlab(self):
        """
    @brief Extracts and sets depth parameters from sample name for KTIMA wetlab procedure.

    @details
    This method parses the sample_id to extract point_id and depth information specific to the KTIMA wetlab procedure.
    The function expects sample names to follow the format "point_id_depth_code" where depth_code indicates
    the soil depth layer:
    - 's' (subsoil): Sets depth range to 20-50 cm
    - 't' (topsoil): Sets depth range to 0-20 cm
    
    The function modifies the following record parameters:
    - self.record_D['point_id']: Extracted from the first part of sample_id (before underscore)
    - self.record_D['min_depth']: Minimum depth based on depth code
    - self.record_D['max_depth']: Maximum depth based on depth code

    @return bool: Returns True if depth code is successfully parsed and depth parameters are set.
                 Returns None if the depth code is not recognized ('s' or 't').

    @note This function is specifically designed for the KTIMA wetlab procedure and expects a specific
          sample naming convention. It must be called after the sample_id has been set in self.record_D.

    @exception Prints error message and returns None if depth code is not 's' or 't'.
               The unrecognized depth code and full sample_id are included in the error message.

    @see _Rearrange_row_data_2_record() for setting up the initial record dictionary structure.
    
    @warning This function will fail if sample_id does not contain an underscore or if the format
             doesn't match the expected "point_id_depth_code" pattern.
    """

        # Set record parameters from the sample name
        self.record_D['point_id'], sample_depth_code = self.record_D['sample_id'].split('_')

        if sample_depth_code.lower() == 's':

            self.record_D['min_depth'], self.record_D['max_depth'] = '20','50'

        elif sample_depth_code.lower() == 't':

            self.record_D['min_depth'], self.record_D['max_depth'] = '0','20'

        else:

            msg = '❌  ERROR - depth code not recognised from sample name: %s' %(self.record_D['sample_id'])

            print (msg)

            return None
        
        return True
        
    def _Set_equipment_method(self):
        """
        @brief Initializes the equipment-method mapping dictionary and related data structures for processing observations.

        @details
        This method performs several critical setup operations for processing CSV data:
        - Validates that all observations use a single equipment type (requirement for AI4SH format).
        - Creates a hierarchical equipment-method dictionary structure where each equipment maps to its associated methods.
        - Initializes empty lists for each method to store observations.
        - Builds mapping dictionaries between indicators, methods, and CSV column headers.
        - Sets up data structures needed for subsequent observation processing.

        The function creates the following instance attributes:
        - self.equipment: Set to the procedure name from process parameters.
        - self.equipment_method_D: Hierarchical dictionary {equipment: {method: []}} for storing observations.
        - self.indicator_in_L: List of observation column names as they appear in the CSV file.
        - self.indicator_L: List of standardized indicator names from parameter dictionary.
        - self.indicator_D: Maps standardized indicator names to CSV column names.
        - self.method_L: List of method column names from the CSV file.
        - self.indicator_method_D: Maps standardized indicator names to their corresponding methods.

        @return bool: Returns True if setup is successful, None if validation fails.
                    Returns None specifically when multiple equipment types are detected in a single record,
                    which violates the AI4SH data format requirement.

        @note This function must be called after _Distill_parameters() has populated the method_D, equipment_D, 
            and parameter_D dictionaries. The AI4SH format requires that each record contains observations 
            from only one equipment type.

        @exception Prints error message and returns None if multiple equipment types are found in the same record.
                This is a validation failure that prevents further processing of the current data row.

        @see _Distill_parameters() for setting up the required dictionaries.
        @see _Get_observation_measurements_AI4SH() for using the equipment_method_D structure.
        """

        unique_equipment_set = set(self.equipment_D.values())

        if (len(unique_equipment_set)) != 1:

            print(' ❌ ERROR - each record can only have a singular equipment, found:', unique_equipment_set)

            return None

        self.equipment = self.process_parameters.procedure

        self.equipment_method_D = {self.equipment: {}}
        
        for key in self.method_D:

            self.equipment_method_D[self.equipment][self.method_D[key]] = []
                
        # Create a list of the observations as they appear in the csv file
        self.indicator_in_L = list(self.equipment_D.keys())

        # Conver the list of observations to the correct indicator name as given in the csv header file
        self.indicator_L = [self.parameter_D[key] for key in self.indicator_in_L]
            
        # Create a dictionary to map the indicator names to the csv header names
        self.indicator_D = dict(zip(self.indicator_L,self.indicator_in_L))

        self.method_L = list(self.method_D.keys())

        # Create a dictionary to map the method names to the indicator names
        self.indicator_method_D = dict(zip(self.indicator_L,self.method_L))

        return True

    def _Assemble_ossl_csv(self):

        self.record_D['license'] = 'only for use by AI4SH'
  
        self.record_D['doi'] = "not_yet_published"

        self.record_D['instrument_setting'] = "not recorded"

        self.record_D['depth'] = '%s-%s' %(self.record_D['min_depth'],self.record_D['max_depth'])

        self.record_D['instrument_model__name'] = 'wetlab'

        self.record_D['instrument_id'] = 'unknown'
  
        ossl_csv_value_L = [self.record_D[item] for item in AI4SH_Key_L]

        ossl_csv_value_L.extend(list(self.record_D['value'].values()))

        self.ossl_values_L.append(ossl_csv_value_L)

    def _Convert_wetlab_to_OSSL(self):

        header_L = deepcopy(AI4SH_Key_L)

        header_L.extend(list(self.record_D['value'].keys()))

        self._Write_OSSL_csv('ai4sh', header_L, self.ossl_values_L)

def Process_ai4sh_csv(project_FP,process, column_L, data_L_L, all_parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D, std_row = None):
    """
    @brief Processes CSV data records and exports them to hierarchical JSON format for AI4SH in-situ data management.

    @details
    This function orchestrates the conversion of CSV data rows into structured JSON sample events. 
    It initializes the json_db class, sets up output folders, cleans column headers, distills parameter dictionaries, 
    checks consistency, and loops through each data record to assemble and export sample events.

    The function performs the following steps:
    - Initializes coordinate data from point name position sample date CSV file.
    - Initializes the json_db class with process parameters and coordinates.
    - Sets up the destination folder for JSON output.
    - Cleans and normalizes column headers.
    - Distills parameter, method, equipment, equipment_model, equipment_id, and unit dictionaries.
    - Checks consistency among method, equipment, and unit mappings.
    - Adds compulsory parameters with default values if missing.
    - Loops through each data record:
        - Converts row data to lowercase.
        - Initializes equipment-method mapping.
        - Converts row data to dictionary format.
        - Rearranges data into observation sub-dictionaries.
        - Handles special procedures (e.g., 'veltia').
        - Sets compulsory record parameters, location data, and coordinates.
        - Validates and finalizes record data.
        - Sets sampling log and sample ID.
        - Creates database objects (point, site, data source).
        - Sets observation metadata and measurements.
        - Assembles sample events in both AI4SH and xSpectre formats.
        - Exports each sample event to JSON files.

    @param project_FP (str): The project file path used for file operations and data reading.
    @param process (object): Object containing process parameters, expected to have a 'parameters' attribute.
    @param column_L (list): List of column header strings from the CSV file.
    @param data_L_L (list): List of lists, each representing a row of data from the CSV file.
    @param all_parameter_D (dict): Dictionary mapping column headers to parameter names.
    @param unit_D (dict): Dictionary mapping column headers to unit names.
    @param method_D (dict): Dictionary mapping column headers to method names.
    @param equipment_D (dict): Dictionary mapping column headers to equipment names.
    @param equipment_model_D (dict): Dictionary mapping column headers to equipment model names.
    @param equipment_id_D (dict): Dictionary mapping column headers to equipment ID values.
    @param std_row (optional): Standard deviation row or index, if available. Defaults to None.

    @return None: The function does not return a value but creates JSON files for each sample event.
               Returns None immediately if any critical error occurs during processing (e.g., coordinate file reading fails,
               parameter distillation fails, consistency checks fail, or JSON assembly fails).

    @note This function expects the coordinate CSV file to be accessible via process.parameters.point_name_position_sampledate_FPN.
          The function creates two JSON formats for each sample: AI4SH format and xSpectre format.
          
    @exception Prints detailed error messages for various failure conditions:
        - Coordinate file reading failures
        - Parameter dictionary inconsistencies  
        - Missing compulsory parameters
        - JSON assembly errors
        - File I/O errors during JSON export
    """

    coordinate_D = Coordinates_fix(project_FP,process.parameters.point_name_position_sampledate_FPN)

    if not coordinate_D:

        print ('❌  ERROR - reading the location, setting, sample date and coordinate csv file failed.')

        print('❌  File: %s' % (process.parameters.point_name_position_sampledate_FPN))

        return None
            
    # Initiate the json_db class
    json_db_C = json_db(project_FP,process, coordinate_D)

    # Initialize the ossl values list - only  if the output data is to be used in OSSl format
    json_db_C.ossl_values_L = []

    # Set and create the destination folder if it doesn't exist
    json_db_C._Set_dst_FP()

    # Clean and set the column headers
    json_db_C._Set_column_L(column_L)

    # List of columns headers to loop over
    parameter_id_L =['parameter','method','equipment','equipment_model','equipment_id','unit']

    # Distill the parameter, method, equipment and unit dictionaries
    for i, item in enumerate([all_parameter_D, method_D, equipment_D, equipment_model_D, equipment_id_D, unit_D]):
 
        result = json_db_C._Distill_parameters( item, parameter_id_L[i])

        if not result:

            return None

    # Check that the method, equipment and unit parameters are consistent
    result = json_db_C._Check_parameter_consistency()

    if not result:

        return None

    # Add compulsary parameters with default values if they are not set in the csv data file
    json_db_C._Add_compulsary_default_parameters()

    # Loop all the data records in the csv data file
    for data_row in  data_L_L:

        # Create a hierarchical dictionary to hold equipment -> methods (must be recreated in each loop)
        data_row = json_db_C._Convert_to_lower(data_row)

        # Create a hierarchical dictionary to hold equipment -> methods (must be recreated in each loop)
        success = json_db_C._Set_equipment_method()

        if not success:

            return None

        # Convert the csv data row to a dict using the csv header records as keys
        json_db_C._Row_data_to_dict(data_row)

        # Rearrange the initial dictionary to hold the observations in a sub dictionary
        json_db_C._Rearrange_row_data_2_record()

        if json_db_C.process.parameters.procedure == 'veltia':

            success = json_db_C._Sample_name_parameters_ktima_wetlab()

            if not success:

                return None

        # ===== Reorganise all inpit data into a single record dictionary =====

        # Check that all compulsory parameters are set in the row data dictionary 
        # Missing parameters are set from the process parameters dictionary
        result = json_db_C._Check_set_compulsary_record_parameters()

        # Set the location (locus) sample date and coordinate parameters
        # These must be given in a separate csv file
        result = json_db_C._Set_locus_sample_date_coordinate()

        if not result:

            return None
        
        # Check the final record and set calculated parameters
        success = json_db_C._Check_set_final_record(project_FP)

        if not success:

            return None
        
        # Set the sampling log parameters
        json_db_C._Set_sampling_log()

        # Set sample id
        json_db_C._Set_sample_id()

        # ===== Set the DB output objects =====

        # Set the db object point
        json_db_C._Set_point()

        # Set the db object site
        json_db_C._Set_site()

        # Set the db object data_soruce (= pilot for AI4SH)
        json_db_C._Set_data_source()

        # Set the observation metadata
        result = json_db_C._Set_observation_metadata()

        if not json_db_C.observation_metadata['subsample']: 
            
            print (' ⚠️  Skipping sample <%s> as subsample is set to None' %(json_db_C.record_D['sample_id']))

            continue
        
        # Attach the observation measurements to the equipment_method_D dictionary 
        json_db_C._Get_observation_measurements_AI4SH()

        # Set the sample parameters
        json_db_C._Set_sample()

        # Assemble the complete record to a final dictionary in AI4SH format
        sample_event_ai4sh = json_db_C._Assemble_sample_event_AI4SH_AI4SH()

        if sample_event_ai4sh:

            # Dump the complete sample event to a JSON file
            json_db_C._Dump_sample_json(sample_event_ai4sh, 'ai4sh')

        else:

            print('❌ Error creating JSON pos for AI4SH from AI4SH')

            return None
        
        # Assemble the complete record to a final dictionary in xspectre format
        sample_event_xspectre = json_db_C._Assemble_sample_event_to_xspectre_from_ai4sh()

        if sample_event_xspectre:

            # Dump the complete sample event to a JSON file
            json_db_C._Dump_sample_json(sample_event_xspectre, 'xspectre')

        else:

            print('❌ Error creating JSON post')

            return None
        
        # if json_db_C.process_parameters_D['procedure'] == 'wetlab':
        
        #    json_db_C._Assemble_ossl_csv()

        #if process.parameters.procedure == 'digit-soil-sear':
        #    pass
        #    ''' 
        #    THIS PART IS FOR DEFINING SAMPLE DATES
        #    items = list(json_db_C.record_D['position_date'].values())

        #    items_str = ",".join(items)

        #    position_date_F.write(items_str + "\n")
        #    '''      

    #if json_db_C.process_parameters_D['procedure'] == 'wetlab':

    #    sample_event_OSSL = json_db_C._Convert_wetlab_to_OSSL()

    #position_date_F.close()