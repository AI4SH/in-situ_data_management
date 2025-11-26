'''
Created on 14 jan 2025
Last updated on 25 aug 2025 (complete rewrite to object oriented class structure)
Updated on 1 Sept 2025 (doxygen comments added)
Updated 1 October - split out to common class for xspectre and ai4sh json structure

@author: thomasgumbricht
'''

# Standard library imports
from copy import deepcopy
from os import path

# Package application imports
from src.utils import  Full_path_locate, Read_csv, Remove_path

from src.lib import Coordinates_fix

from .common import common_json_db

# Default variables
COMPULSARY_DATA_RECORDS = ['pilot_country','pilot_site','point_id','min_depth','max_depth','sample_date',
                                'sample_preparation__name','subsample','replicate','sample_analysis_date','sample_preservation__name',
                                'sample_transport__name','transport_duration_h','sample_storage__name','user_analysis__email',
                                'user_sampling__email','user_logistic__email', 'procedure']

PARAMETERS_WITH_ASSUMED_DEFAULT = ['sample_preparation__name','sample_preservation__name','sample_transport__name','sample_storage__name','transport_duration_h','replicate','subsample']

ASSUMED_DEFAULT_VALUES = [None,None,None,None,0,0,'a']

AI4SH_Key_L = ["pilot_site",
               "point_id",
               "depth",
               "sample_id",
               "sample_preparation__name",
               "subsample",
               "replicate",  
               "instrument-model__name",
                "instrument_id",
                "instrument_setting",
                "sample_analysis_date",
                "license",
                "doi",
                "user_analysis__email"]

class json_db(common_json_db):
    """
    @class json_db
    @brief Handles the processing and structuring of xspetre json data records for AI4SH in-situ data management, exporting them to hierarchical JSON format.

    @details
    The json_db class provides methods to:
    - Initialize with process parameters.
    - Set up destination folders for output.
    - Add compulsory parameters with default values if missing.
    - Ensure all compulsory record parameters are present.
    - Map equipment to methods and store observations.
    - Extract and process observation measurements.
    - Generate unique identifiers for pilot, site, point, and sample.
    - Set metadata for observations, samples, and sampling logs.
    - Assemble hierarchical sample event dictionaries for JSON export.
    - Write sample event data to JSON files.
    """

    def __init__(self, project_FP,process, coordinate_D):
        """
        @brief Constructor for the json_db class, recursively initializes a Struct object from a dictionary.
   
        @details
        - Extracts and stores the process parameters from the provided process object.
        - Converts the process parameters to a dictionary for easier access and manipulation.

        @param process An object containing process parameters, expected to have a 'parameters' attribute.

        @return None
        """

        common_json_db.__init__(self, project_FP,process, coordinate_D)

    def _Set_column_L(self, column_L):
        """
        @brief Cleans and normalizes a list of column headers.

        This function processes the input list of column headers by:
        - Removing any leading Byte Order Mark (BOM) characters and whitespace.
        - Converting all column names to lowercase.
        - Replacing values that indicate missing data (e.g., 'na', 'none', 'n/a', 'nan', '', 'null') with None.

        @param column_L List of column header strings to be cleaned and normalized.

        @return None. The cleaned list is assigned to self.column_L.
        """

        self.column_L = [col.replace('\ufeff','').strip().lower() for col in column_L]

        self.column_L = [None if col in ['na', 'none', 'n/a', 'nan', '', 'null'] else col for col in self.column_L]

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
        @brief Converts a row of data into a dictionary using column headers as keys.

        This function takes a list of row data and maps each value to its corresponding column name
        from self.column_L, creating a dictionary where keys are column names and values are row entries.
        The resulting dictionary is assigned to self.record_D.

        @param row_data List of values representing a single row of data.

        @return None. The resulting dictionary is stored in self.record_D.
        """

        self.record_D = dict(zip(self.column_L, row_data))

        self.record_D['n_repetitions'] = 1

    def _Rearrange_row_data_2_record(self):

        self.record_D['value'] = {}

        self.record_D['standard_deviation'] = {}

        #self.record_D['n_repetitions'] = {}

        self.record_D['unit__name'] = {}

        self.record_D['indicator__name'] = {}

        self.record_D['procedure'] = {}

        self.record_D['analysis_method__name'] = {}

        self.record_D['instrument-brand__name'] = {}

        self.record_D['instrument-model__name'] = {}

        self.record_D['instrument_id'] = {}

        for ind in self.indicator_L:

            if ',' in self.record_D[self.indicator_D[ind]]:
                                    
                self.record_D[self.indicator_D[ind] ] = self.record_D[self.indicator_D[ind]].replace(',','.')
                          
            try:

                self.record_D['value'][ind] = float(self.record_D[self.indicator_D[ind]])

            except (ValueError, TypeError):

                self.record_D['value'][ind] = None

            self.record_D['standard_deviation'][ind] = None

            #self.record_D['n_repetitions'][ind] = 1

            self.record_D['unit__name'][ind] = self.unit_D[self.indicator_D[ind]]

            self.record_D['indicator__name'][ind] = self.parameter_D[self.indicator_D[ind]]

            if 'pilot_country' in self.record_D:

                # Convert pilot_country, pilot_site and sample_id to lower case and replace spaces with '-'
                self.record_D['pilot_country'] = self.record_D['pilot_country'].lower().replace(' ','-')
            if 'pilot_site' in self.record_D:
                
                self.record_D['pilot_site'] = self.record_D['pilot_site'].lower().replace(' ','-')
            
            self.record_D['point_id'] = self.record_D['sample_id'].lower().replace(' ','-')

            for item in ['procedure','analysis_method__name','instrument-brand__name','instrument-model__name','instrument_id']:
                
                try:
                    if item == 'procedure':

                        self.record_D[item][ind] = '%s-%s' %(self.method_D[self.indicator_D[ind]],ind)

                    if item == 'analysis_method__name':

                        self.record_D[item][ind] = '%s-%s' %(self.method_D[self.indicator_D[ind]],ind)
                   
                    elif item == 'instrument-brand__name':

                        self.record_D[item][ind] = '%s-%s' %(self.method_D[self.indicator_D[ind]],ind)
                   
                    elif item == 'instrument-model__name':

                        self.record_D[item][ind] = self.equipment_model_D[self.indicator_D[ind]]
                    
                    elif item == 'instrument_id':

                        self.record_D[item][ind] = self.equipment_id_D[self.indicator_D[ind]]

                except KeyError:

                    self.record_D[item][ind] = None

                #if self.record_D[item][ind].lower in ['na', 'none', 'n/a', 'nan', '', 'null','unknwon']:

                #    self.record_D[item][ind] = None

                if self.record_D[item][ind].lower() in ['unknown', 'null']:

                    if item in self.process_parameters_D:
                            
                        self.record_D[item][ind] = self.process_parameters_D[item]

    def _Sample_name_parameters_ktima_wetlab(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

       # print (self.record_D['sample_id'])
        # Set record parameters from the sample name
        self.record_D['point_id'], sample_depth_code = self.record_D['sample_id'].split('_')
        #self.record_D['point_id'] = self.record_D['sample_id']
        if sample_depth_code.lower() == 's':

            self.record_D['min_depth'], self.record_D['max_depth'] = '20','50'

        elif sample_depth_code.lower() == 't':

            self.record_D['min_depth'], self.record_D['max_depth'] = '0','20'

        else:

            msg = '❌  ERROR - depth code not recognised in sample name: %s' %(sample_name)

            print (msg)

            return None
        
    def _Add_compulsary_default_parameters(self):
        """
        @brief Adds compulsory parameters with assumed default values to the process parameters dictionary if they are missing.

        This function checks for each parameter listed in PARAMETERS_WITH_ASSUMED_DEFAULT whether it exists in self.process_parameters_D.
        If a parameter is missing, it assigns the corresponding value from ASSUMED_DEFAULT_VALUES and prints a warning message.

        @return None
        """

        default_D = dict(zip(PARAMETERS_WITH_ASSUMED_DEFAULT, ASSUMED_DEFAULT_VALUES))

        for parameter in default_D:

            if not parameter in self.process_parameters_D:

                self.process_parameters_D[parameter] = default_D[parameter]

                print (' ⚠️ WARNING - assuming default value <%s> for  parameter <%s>' %(default_D[parameter], parameter))

    def _Check_set_compulsary_record_parameters(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the 'pilot' column is missing,
        it assumes its value is equal to 'pilot_site' and issues a warning. For each compulsory parameter, if it is missing
        or empty in the row data, the function attempts to set it from the process parameters dictionary. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - If 'pilot' is not present in the column list, its value is set to 'pilot_site'.
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty in row_data_D.
        - If a compulsory parameter is missing, attempts to set it from process_parameters_D.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """
        ''''
        self.pilot_site = '<PILOT_SITE>'

        if not 'pilot' in self.column_L:

            if self.record_D['pilot_site'] != self.pilot_site:

                self.pilot_site = self.record_D['pilot_site'].lower().replace(' ','-')

                print (' ⚠️ WARNING - assuming value for <pilot> is equal to <pilot_site>: '+self.pilot_site)

            self.record_D['pilot'] =  self.record_D['pilot_site']
        '''
        for item in COMPULSARY_DATA_RECORDS:

            if item not in self.record_D or len(self.record_D[item]) == 0:

                if item in self.process_parameters_D:

                    self.record_D[item] = self.process_parameters_D[item]

                else:

                    print ('❌  ERROR - compulsory data not found: '+item)

                    print (' You can add <'+item+'> parameter to either:')

                    print('  - the method header definition file: %s' %(self.process_parameters.method_src_FPN))
                    
                    print('  - or the data file: %s' %(self.process_parameters.data_src_FPN))

                    return None

        return True

    def _Set_equipment_method(self):
        """
        @brief Initializes the equipment-method mapping dictionary.

        @details
        - Creates a dictionary (equipment_method_D) where each unique equipment is a key, and its value is a dictionary of methods associated with that equipment.
        - For each method in method_D, associates it with the corresponding equipment in equipment_D and initializes an empty list for storing observations.

        @return None
        """

        unique_equipment_set = set(self.equipment_D.values())

        if (len(unique_equipment_set)) != 1:

            print(' ❌ ERROR - each record can only have a singular equipment, found:', unique_equipment_set)

            return None
        
        #self.equipment_method_D = dict.fromkeys(unique_equipment_set, {})

        #self.equipment = list(self.equipment_method_D.keys())[0]

        self.equipment = self.process_parameters.procedure

        self.equipment_method_D = {self.equipment: {}}
        
        #self.equipment_method_D = dict.fromkeys(unique_equipment_set, {})
        
        for key in self.method_D:
        
            #self.equipment_method_D[self.equipment_D[key]][self.method_D[key]] = []

            self.equipment_method_D[self.equipment][self.method_D[key]] = []
                
        '''
        # convert the dictionary of equipment to a set of unique equipment
        unique_equipment_set = set(self.equipment_D.values())
        
        # For each row in the csv all observations must come from the same equipment
        if (len(unique_equipment_set)) != 1:

            print(' ❌ ERROR - each record can only have a singular equipment, found:', unique_equipment_set)

            return None

        # Retrieve the unique equipment and create the equipment_method_D dictionary
        self.equipment_method_D = dict.fromkeys(unique_equipment_set, {})

        self.equipment = list(self.equipment_method_D.keys())[0]

        self.equipment_method_D = {self.equipment: {}}

        # Create the method keys for the single equipment, it will hold a list of observations
        self.equipment_method_D[self.equipment ] = []
        '''
        # Create a list of the observations as they appear in the csv file
        self.indicator_in_L = list(self.equipment_D.keys())

        # Conver the list of observations to the correct indicator name as given in the csv header file
        self.indicator_L = [self.parameter_D[key] for key in self.indicator_in_L]
            
        # Create a dictionary to map the indicator names to the csv header names
        self.indicator_D = dict(zip(self.indicator_L,self.indicator_in_L))

        self.method_L = list(self.method_D.keys())

        # Create a dictionary to map the method names to the indicator names
        self.indicator_method_D = dict(zip(self.indicator_L,self.method_L))

    def _Assemble_ossl_csv(self):

        self.record_D['license'] = 'only for use by AI4SH'
  
        self.record_D['doi'] = "not_yet_published"

        self.record_D['instrument_setting'] = "not recorded"

        self.record_D['depth'] = '%s-%s' %(self.record_D['min_depth'],self.record_D['max_depth'])

        self.record_D['instrument-model__name'] = 'wetlab'

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

    - Initializes the json_db class with process parameters.
    - Sets up the destination folder for JSON output.
    - Cleans and normalizes column headers.
    - Distills parameter, method, equipment, and unit dictionaries.
    - Checks consistency among method, equipment, and unit mappings.
    - Adds compulsory parameters with default values if missing.
    - Loops through each data record:
        - Initializes equipment-method mapping.
        - Converts row data to dictionary.
        - Ensures all compulsory parameters are set.
        - Extracts and processes observation measurements.
        - Sets unique identifiers for pilot, site, point, and sample.
        - Sets observation metadata, sample, and sampling log.
        - Assembles the sample event and exports to JSON.

    @param process Object containing process parameters, expected to have a 'parameters' attribute.
    @param column_L List of column header strings from the CSV file.
    @param data_L_L List of lists, each representing a row of data from the CSV file.
    @param all_parameter_D Dictionary mapping column headers to parameter names.
    @param unit_D Dictionary mapping column headers to unit names.
    @param method_D Dictionary mapping column headers to method names.
    @param equipment_D Dictionary mapping column headers to equipment names.
    @param std_row (Optional) Standard deviation row or index, if available.

    @return None if any error occurs during processing, otherwise creates JSON files for each sample event.
    """

    #global position_date_F

    coordinate_D = Coordinates_fix(project_FP,process.parameters.coordinates_FPN)

    #position_date_FPN = "/Users/thomasgumbricht/projects/ai4sh_sueloanalys/coordinates/AI4SH_point_name_position_sampledate"
    
    #position_date_FPN = '%s_%s.csv' % (position_date_FPN, process.parameters.pilot_site.lower())

    #position_date_F = open(position_date_FPN, 'w')
    #position_date_F.write('pilot_country,pilot_site,sampling_log,point_id,sample_date,min_depth,max_depth,position_name,setting,latitude,longitude\n')

    if process.overwrite:

        print('Overwrite is set to True, all existing JSON files will be deleted before processing new data')

        for item in ['ai4sh','xspectre']:

            dst_FP= '%s_%s' %(process.parameters.dst_FP,item)

            dst_FP = Full_path_locate(project_FP, dst_FP, True)

            Remove_path(dst_FP)
            
    # Initiate the json_db class
    #json_db_C = json_db(project_FP,process)
    json_db_C = json_db(project_FP,process, coordinate_D)

    json_db_C.ossl_values_L = []

    # Set and create the destination folder if it doesn't exist
    json_db_C._Set_dst_FP()

    # Clean and set the column headers
    json_db_C._Set_column_L(column_L)

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
        json_db_C._Set_equipment_method()

        # Convert the csv data row to a dict using the csv header records as keys
        json_db_C._Row_data_to_dict(data_row)

        # Rearrange the initial dictionary to hold the observations in a sub dictionary
        json_db_C._Rearrange_row_data_2_record()

        if json_db_C.process.parameters.procedure == 'veltia':

            json_db_C._Sample_name_parameters_ktima_wetlab()

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

        # Set the point_id - must be before _Set_sampling_log
        #json_db_C._Set_site_point_id()

        # Set the sampling log - must be before _Set_sample
        #json_db_C._Set_sampling_log()

        # Set the sample parameters
        #json_db_C._Sample_name_parameters_ktima_wetlab()
        #json_db_C._Set_sample()

        # Set the observation metadata
        #result = json_db_C._Set_observation_metadata()

        #if not result:

        #    return None
        # Set the ids for pilot, site, point and sample
        #result = json_db_C._Set_pilot_site_point()

        #json_db_C._Set_analysis_method()
        
        # Attach the observation measurements to the equipment_method_D dictionary 
        json_db_C._Get_observation_measurements_AI4SH()

        # Set the sample parameters
        json_db_C._Set_sample()

        # Assemble the complete sample event to a final dictionary containing all parameters
        sample_event_ai4sh = json_db_C._Assemble_sample_event_AI4SH_AI4SH()

        if sample_event_ai4sh:

            # Dump the complete sample event to a JSON file
            json_db_C._Dump_sample_json(sample_event_ai4sh, 'ai4sh')

        else:

            print('❌ Error creating JSON pos for AI4SH from AI4SH')

            return None
        
        # Assemble the complete sample event to a final dictionary containing all parameters
        sample_event_xspectre = json_db_C._Assemble_sample_event_to_xspectre_from_ai4sh()

        if sample_event_xspectre:

            # Dump the complete sample event to a JSON file
            json_db_C._Dump_sample_json(sample_event_xspectre, 'xspectre')

        else:

            print('❌ Error creating JSON post')

            return None
        
        if json_db_C.process_parameters_D['procedure'] == 'wetlab':
        
            json_db_C._Assemble_ossl_csv()

        if process.parameters.procedure == 'digit-soil-sear':
            pass
            ''' 
            THIS PART IS FOR DEFINING SAMPLE DATES
            items = list(json_db_C.record_D['position_date'].values())

            items_str = ",".join(items)

            position_date_F.write(items_str + "\n")
            '''      

    if json_db_C.process_parameters_D['procedure'] == 'wetlab':

        sample_event_OSSL = json_db_C._Convert_wetlab_to_OSSL()

    #position_date_F.close()

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
        
        equipment_model_D = dict(zip(column_D['header'], column_D['equipment_id']))
    else:
        equipment_id_D = dict.fromkeys(column_D['header'], 'unknown')

    return parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D

def Data_read(project_FP,data_FPN):

    data_FPN = Full_path_locate(project_FP, data_FPN)
   
    if not path.exists(data_FPN):

        print ("❌ The data path does not exist: %s" % (data_FPN))

        return None

    data_pack = Read_csv(data_FPN)

    if not data_pack:
        
        return None
    
    return data_pack