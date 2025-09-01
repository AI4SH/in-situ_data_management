'''
Created on 14 jan 2025
Last updated on 25 aug 2025

@author: thomasgumbricht
'''

# Standard library imports
from os import path, makedirs

# Package application imports
from src.utils import  Read_csv, Delta_days, Dump_json

# Default variables
COMPULSARY_DATA_RECORDS = ['pilot_country','pilot','pilot_site','sample_id','min_depth','max_depth','sample_date',
                                'sample_preparation__name','subsample','replicate','sample_analysis_date','sample_preservation__name',
                                'sample_transport__name','transport_duration_h','sample_storage__name','user_email_analysis',
                                'user_email_sampling','user_email_logistic']

PARAMETERS_WITH_ASSUMED_DEFAULT = ['sample_preparation__name','sample_preservation__name','sample_transport__name','sample_storage__name','transport_duration_h','replicate','subsample']

ASSUMED_DEFAULT_VALUES = [None,None,None,None,0,0,'a']

REPLICATE_D = {0:0, '0':0, '1':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9, 
               'a': 0, 'b':1,'c':2,'d':3,'e':4,'f':5,'g':6,'h':7,'i':8,'j':9,'k':10,
                'ab':2,'ac':3} 

SUBSAMPLE_D = {'None':None,'a':"a",'b':"b",'c':"c",'d':"d",'e':"e",'f':"f",'g':"g",'h':"h",'i':"i",'j':"j",'k':"k",
               'a1':"a",'a2':"b",'a3':"c",'a4':"d",'b1':"e",'b2':"f",'b3':"g",
                  'c1':"h",'c2':"i",'c3':"j",'d1':"k",'d2':"l",'d3':"m"}

class json_db:
    """
    @class json_db
    @brief Handles the processing and structuring of CSV data records for AI4SH in-situ data management, exporting them to hierarchical JSON format.

    @details
    The json_db class provides methods to:
    - Initialize with process parameters.
    - Set up destination folders for output.
    - Clean and normalize column headers.
    - Distill and validate parameter, method, equipment, and unit dictionaries.
    - Check consistency among method, equipment, and unit mappings.
    - Convert row data to dictionaries using column headers.
    - Add compulsory parameters with default values if missing.
    - Ensure all compulsory record parameters are present.
    - Map equipment to methods and store observations.
    - Extract and process observation measurements from data rows.
    - Generate unique identifiers for pilot, site, point, and sample.
    - Set metadata for observations, samples, and sampling logs.
    - Assemble hierarchical sample event dictionaries for JSON export.
    - Write sample event data to JSON files.
    """

    def __init__(self, process):
        """
        @brief Initializes the json_db class with process parameters.

        @details
        - Extracts and stores the process parameters from the provided process object.
        - Converts the process parameters to a dictionary for easier access and manipulation.

        @param process An object containing process parameters, expected to have a 'parameters' attribute.

        @return None
        """
        self.process_parameters = process.parameters
        self.process_parameters_D = dict(list(process.parameters.__dict__.items()))

    def _Set_dst_FP(self):
        """
        @brief Sets the destination folder path and creates it if it does not exist.

        This function assigns the destination folder path from the process parameters to the instance variable `dst_FP`.
        If the folder does not exist, it creates the directory using `os.makedirs`.

        @details
        - Retrieves the destination folder path from `self.process_parameters.dst_FP`.
        - Checks if the folder exists using `os.path.exists`.
        - Creates the folder if it does not exist.

        @return None
        """
        self.dst_FP = self.process_parameters.dst_FP
        if not path.exists(self.dst_FP):
            makedirs(self.dst_FP)

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

    def _Row_data_to_dict(self, row_data):
        """
        @brief Converts a row of data into a dictionary using column headers as keys.

        This function takes a list of row data and maps each value to its corresponding column name
        from self.column_L, creating a dictionary where keys are column names and values are row entries.
        The resulting dictionary is assigned to self.row_data_D.

        @param row_data List of values representing a single row of data.

        @return None. The resulting dictionary is stored in self.row_data_D.
        """
        self.row_data_D = dict(zip(self.column_L, row_data))

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
        self.pilot_site = '<PILOT_SITE>'

        if not 'pilot' in self.column_L:

            if self.row_data_D['pilot_site'] != self.pilot_site:

                self.pilot_site = self.row_data_D['pilot_site']

                print (' ⚠️ WARNING - assuming value for <pilot> is equal to <pilot_site>: '+self.pilot_site)

            self.row_data_D['pilot'] =  self.row_data_D['pilot_site']

        for item in COMPULSARY_DATA_RECORDS:

            if item not in self.row_data_D or len(self.row_data_D[item]) == 0:

                if item in self.process_parameters_D:

                    self.row_data_D[item] = self.process_parameters_D[item]

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
        self.equipment_method_D = dict.fromkeys(unique_equipment_set, {})
        for key in self.method_D:
            self.equipment_method_D[self.equipment_D[key]][self.method_D[key]] = []

    def _Get_observation_measurements(self, data_row, sd_column = None):
        """
        @brief Extracts observation measurements from a data row and appends them to the equipment-method mapping.

        This function iterates over the columns in the data row, identifies measurement columns based on the method dictionary,
        and processes their values. It handles missing values, values with '<' (interpreted as half the threshold), and optionally
        includes standard deviation if provided. The processed observation is appended to the corresponding equipment-method list.

        @param data_row List of values representing a single row of measurement data.
        @param sd_column (Optional) Index of the column containing standard deviation values. If provided, standard deviation is included in the observation dictionary.

        @details
        - For each measurement column:
        - If the value is empty, it is set to None.
        - If the value starts with '<', it is converted to half the threshold value.
        - The value and standard deviation (if available) are converted to float, with ',' replaced by '.'.
        - The observation dictionary includes value, unit name, indicator name, lab analysis method name, and optionally standard deviation.
        - The observation is appended to the equipment-method mapping dictionary.

        @return None
        """
        for  m, key in enumerate(self.column_L):

            if key in self.method_D:

                if len(data_row[m]) == 0:

                    data_row[m] = None
                
                elif data_row[m][0] == '<':

                    data_row[m] = str(float(data_row[m][1:])/2)

                if sd_column:

                    observation_D =  {'value': None if data_row[m] == 'None' else float(data_row[m].replace(',','.')), 
                                'standard_deviation': None if data_row[sd_column] == 'None' else float(data_row[sd_column].replace(',','.')),
                                'unit__name': self.unit_D[key],
                                'indicator__name': self.parameter_D[key],
                                'lab_analysis_method__name': self.method_D[key]}

                else:

                    observation_D =  {'value': None if data_row[m] == 'None' else float(data_row[m].replace(',','.')),
                                'unit__name': self.unit_D[key],
                                'indicator__name': self.parameter_D[key],
                                'lab_analysis_method__name': self.method_D[key]}

                self.equipment_method_D[self.equipment_D[key]][self.method_D[key]].append(observation_D)

    def _Set_pilot_site_point_sample_id(self):
        """
        @brief Sets unique identifiers for pilot, site, point, and sample, and initializes related metadata dictionaries.

        This function processes the current row data to generate normalized and unique IDs for pilot, site, point, and sample.
        It also creates metadata dictionaries for point, site, and data source, which are used in subsequent data structuring.

        @details
        - Normalizes and formats the 'pilot', 'pilot_site', and 'sample_id' fields from the row data.
        - Constructs a unique sample ID using site, point, depth, and date information.
        - Initializes dictionaries for point, site, and data source metadata.

        @return None
        """
        
        self.pilot_id = self.row_data_D['pilot'].replace(' ', '-').replace('_', '-').lower()
        self.site_id = self.row_data_D['pilot_site'].replace(' ', '-').replace('_', '-').lower()
        self.point_id = self.row_data_D['sample_id'].replace(' ', '-').replace('_', '-').lower()

        self.sample_id =  self.site_id+'_'+self.point_id+\
                    '_'+self.row_data_D['min_depth']+\
                    '-'+self.row_data_D['max_depth']+\
                        '_'+self.row_data_D['sample_date']

        self.point = {
            "name": self.point_id
        }

        self.site = {
            "name": self.site_id
        }

        self.data_source = {
            "name": '%s_%s' % ('ai4sh',self.row_data_D['pilot_country'].lower())
        }

    def _Set_observation_metadata(self):
        """
        @brief Sets metadata for an observation, including sample preparation, user emails, subsample, replicate, date, and logistics.

        This function validates the 'subsample' and 'replicate' fields in the row data against predefined dictionaries (SUBSAMPLE_D and REPLICATE_D).
        If either value is not recognized, an error message is printed and None is returned. Otherwise, it assembles a metadata dictionary
        containing sample preparation details, user emails, subsample and replicate identifiers, date stamp, and a nested logistics dictionary
        with preservation, transport, storage, and logistic user information. The resulting metadata is assigned to self.observation_metadata.

        @details
        - Checks validity of 'subsample' and 'replicate' fields.
        - Assembles observation metadata including sample preparation, user emails, subsample, replicate, date, and logistics.
        - Calculates storage duration in hours using Delta_days between sample and analysis dates.

        @return True if metadata is set successfully, None if validation fails.
        """
        if not self.row_data_D['subsample'] in SUBSAMPLE_D:
            print ('❌  ERROR - subsample id not recognised: %s' % self.row_data_D['subsample'])

            return None

        if not self.row_data_D['replicate'] in REPLICATE_D:
            print ('❌  ERROR - replicate id not recognised: %s' % self.row_data_D['replicate'])

            return None

        self.observation_metadata = {
                        "sample_preparation__name": self.row_data_D['sample_preparation__name'],
                        "person__email": self.row_data_D['user_email_analysis'],
                        "subsample": SUBSAMPLE_D[self.row_data_D['subsample']],
                        "replicate": REPLICATE_D[self.row_data_D['replicate']],
                        "date_stamp": self.row_data_D['sample_date'],
                        "logistic": {
                          "sample_preservation__name": self.row_data_D['sample_preservation__name'],
                          "sample_transport__name": self.row_data_D['sample_transport__name'],
                          "transport_duration_h": self.row_data_D['transport_duration_h'],
                          "sample_storage__name": self.row_data_D['sample_storage__name'],
                          "storage_duration_h":  24*(Delta_days(self.row_data_D['sample_date'],self.row_data_D['sample_analysis_date'] )),
                          "person__email": self.row_data_D['user_email_logistic']}
                        }
        return True

    def _Set_sample(self):
        """
        @brief Sets the sample metadata dictionary for the current data row.

        This function initializes the `self.sample` attribute as a dictionary containing:
        - The sample name (unique sample ID)
        - Minimum depth
        - Maximum depth

        The values for min_depth and max_depth are extracted from the current row data and converted to integers.

        @details
        - Uses `self.sample_id` for the sample name.
        - Retrieves `min_depth` and `max_depth` from `self.row_data_D` and casts them to integers.

        @return None. The sample dictionary is assigned to `self.sample`.
        """

        self.sample = {"name": self.sample_id,
                    "min_depth": int(self.row_data_D['min_depth']),
                    "max_depth": int(self.row_data_D['max_depth'])}
        
    def _Set_sampling_log(self):
        """
        @brief Sets the sampling log metadata for the current sample.

        This function generates a unique sampling log ID using the site ID, point ID, and sample date from the current row data.
        It then creates a sampling log dictionary containing:
        - The sampling log name (unique ID)
        - The date stamp of sampling
        - The email of the person responsible for sampling

        @details
        - Uses self.site_id, self.point_id, and self.row_data_D['sample_date'] to construct the sampling log ID.
        - Populates self.sampling_log with relevant metadata for the sample event.

        @return None. The sampling log dictionary is assigned to self.sampling_log.
        """

        self.sample_log_id = self.site_id+'_'+self.point_id+'_'+self.row_data_D['sample_date']

        self.sampling_log = {"name": self.sample_log_id,
                            "date_stamp": self.row_data_D['sample_date'],
                            "person__email": self.row_data_D['user_email_sampling']}

    def _Assemble_sample_event(self):
        """
        @brief Assembles a hierarchical sample event dictionary for JSON export.

        This function constructs a nested dictionary representing a sample event, including data source, site, point, sampling log, sample, observation, and analysis method. 
        The structure is built from previously set metadata and measurement dictionaries within the class instance.

        @details
        - Combines equipment-method mapping, observation metadata, sample, sampling log, point, site, and data source into a single hierarchical dictionary.
        - Intended for use in exporting structured sample event data to JSON.

        @return Dictionary containing the complete sample event structure, or None if an error occurs during assembly.
        """

        try:

            analysis_method = [self.equipment_method_D]

            observation = [{**self.observation_metadata, "analysis_method": analysis_method}]

            sample = [{**self.sample, "observation": observation}]

            sampling_log = [{**self.sampling_log, "sample": sample}]

            point = [{**self.point, "sampling_log": sampling_log}]

            site = [{**self.site, "point": point}]

            data_source = [{**self.data_source, "site": site}]

            sample_event = {"data_source": data_source}

            return sample_event

        except:

            return None

    def _Dump_sample_json(self, sample_event):
        """
        @brief Dumps a sample event dictionary to a JSON file.

        This function creates a destination file name and path based on the sample ID and destination folder path.
        It then writes the provided sample event dictionary to the JSON file using the Dump_json function with indentation.
        After successful writing, it prints the path of the created JSON file.

        @param sample_event Dictionary containing the sample event data to be exported to JSON.

        @return None
        """
        dst_FN = '%s_x.json' %(self.sample_id)
        dst_FPN = path.join(self.dst_FP, dst_FN)

        # Write the updated json file
        Dump_json(dst_FPN, sample_event, indent=2)

        print('Json post created:',dst_FPN)


def Loop_data_records(process, column_L, data_L_L, all_parameter_D, unit_D, method_D, equipment_D, std_row = None):
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
    # Initiate the json_db class
    json_db_C = json_db(process)

    # Create the destination folder if it doesn't exist
    json_db_C._Set_dst_FP()

    # Clean and set the column headers
    json_db_C._Set_column_L(column_L)

    parameter_id_L =['parameter','method','equipment','unit']

    # Distill the parameter, method, equipment and unit dictionaries
    for i, item in enumerate([all_parameter_D, method_D, equipment_D, unit_D]):
 
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
    for d,data_row in  enumerate(data_L_L):

        # Create a hierarchical dictionary to hold equipment -> methods (must be recreated in each loop)
        json_db_C._Set_equipment_method()

        # Convert the csv data row to a dict using the csv header records as keys
        json_db_C._Row_data_to_dict(data_row)

        # Check that all compulsory parameters are set in in the row data dictionary 
        result = json_db_C._Check_set_compulsary_record_parameters()

        if not result:

            return None

        # Set the observed measurements linked to the correct equipment and method
        json_db_C._Get_observation_measurements(data_row)

        # Set the ids for pilot, site, point and sample
        json_db_C._Set_pilot_site_point_sample_id()

        # Set the observation metadata
        result = json_db_C._Set_observation_metadata()

        if not result:

            return None

        # Set the sample parameters
        json_db_C._Set_sample()

        # Set the sampling log parameters
        json_db_C._Set_sampling_log()

        # Assemble the complete sample event to a final dictionary containing all parameters
        sample_event = json_db_C._Assemble_sample_event()

        if sample_event:

            # Dump the complete sample event to a JSON file
            json_db_C._Dump_sample_json(sample_event)

        else:

            print('❌ Error creating JSON post')

            return None

def Parameters_fix(method_src_FPN):
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

    data_pack = Data_read(method_src_FPN)
    
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
    
    return parameter_D, unit_D, method_D, equipment_D

def Data_read(data_FPN):

    if not path.exists(data_FPN):

        print ("❌ The data path does not exist: %s" % (data_FPN))

        return None

    data_pack = Read_csv(data_FPN)

    if not data_pack:
        
        return None
    
    return data_pack