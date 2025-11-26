'''
Created on 14 jan 2025
Last updated on 25 aug 2025 (complete rewrite to object oriented class structure)
Updated on 1 Sept 2025 (doxygen comments added)
Updated 1 October - split out to common class for xspectre and ai4sh json structure
Updated 24 Nove 2025 - divided subprocesses to single objectives, removed commented out code

@author: thomasgumbricht
'''
from copy import deepcopy

# Package application imports
from src.utils import  Full_path_locate, Remove_path

from src.lib import Coordinates_fix, Interpolate_spectra

from .common import common_json_db

# Default variables
COMPULSARY_DATA_RECORDS = ['pilot_country','pilot_site','point_id','min_depth','max_depth','sample_date',
                                'sample_preparation__name','subsample','replicate','sample_analysis_date','sample_preservation__name',
                                'sample_transport__name','transport_duration_h','sample_storage__name','user_analysis__email',
                                'user_sampling__email','user_logistic__email', 'procedure',
                                'instrument-brand__name','instrument-model__name','instrument_id',
                                'analysis_method__name']

PARAMETERS_WITH_ASSUMED_DEFAULT = ['sample_preparation__name','sample_preservation__name','sample_transport__name','sample_storage__name','transport_duration_h','replicate','subsample']

ASSUMED_DEFAULT_VALUES = [None,None,None,None,0,0,'a']

LOENNSTORP_POINT_ID_D = {'4':'4-a','04':'4-a','5':'5-a','16':'16-a','20':'20-a','24':'24-a',
                         '51':'51-c','55':'55-c','60':'60-c','61':'61-c','72':'72-c',
                         '79':'79-d','80':'80-d','91':'91-d','95':'95-d','99':'99-d',
                         '1-sand':'1-sand','2-sand':'2-sand','3-organic':'3-organic',
                         '4-organic':'4-organic','5-organic':'5-organic','9-sand':'9-sand'}
 
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

        self.column_L = [col.replace('"','') for col in self.column_L]


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

        for i,item in enumerate(self.column_L):

            if i >= 4:

                break

            item = item.replace('\ufeff','').lower()

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

        self.inverse_parameter_D = {v: k for k, v in self.parameter_D.items()}
        
        return True

    def _Get_wavelengths(self,column_L):
        """
        @brief Extracts and processes wavelength data from the record dictionary.

        This function retrieves the 'wavelengths' entry from self.record_D, which is expected to be a comma-separated string of wavelength values.
        It splits this string into a list, converts each value to a float, and assigns the resulting list to self.wavelength_L. If the 'wavelengths'
        entry is missing or cannot be processed, an error message is printed and None is returned.

        @return True if successful, None if an error occurs.
        """
        self.wavelength_L = []

        for w in range(4,len(column_L)):

            self.wavelength_L.append(float(column_L[w].strip()))

    def _Row_data_to_dict(self, row_data):
        """
        @brief Converts a row of data into a dictionary using column headers as keys.

        This function takes a list of row data and maps each value to its corresponding column name
        from self.column_L, creating a dictionary where keys are column names and values are row entries.
        The resulting dictionary is assigned to self.record_D.

        @param row_data List of values representing a single row of data.

        @return None. The resulting dictionary is stored in self.record_D.
        """

        self.record_D = dict(zip(self.column_L[0:4], row_data[0:4]))

        self.record_D['n_repetitions'] = 3
   
        self.record_D['spectra'] = row_data[4:]

    def _Rearrange_row_data_2_record(self):

        self.indicator_L = ['spectra']

        self.indicator_D = {'spectra':'spectra'}

        self.record_D["value"] = {'spectra': self.record_D['spectra']}

        self.record_D["unit__name"] = {}

        for ind in self.indicator_L:

            if ',' in self.record_D[ind][0]:

                self.record_D[ind] = [c.replace(',','.')for c in self.record_D[ind]] 

            try:

                self.record_D['value'][ind] = [float(c)/100 for c in self.record_D[ind]]

            except (ValueError, TypeError):

                self.record_D['value'][ind] = None

            self.record_D['unit__name'][ind] = self.process_parameters.unit__name

            if 'pilot_country' in self.record_D:

                # Convert pilot_country, pilot_site and sample_id to lower case and replace spaces with '-'
                self.record_D["pilot_country"] = self.record_D["pilot_country"].lower().replace(' ','-')
            
            if 'pilot_site' in self.record_D:
                
                self.record_D["pilot_site"] = self.record_D["pilot_site"].lower().replace(' ','-')
            
        if 'canopy' in self.process_parameters_D:

            self.record_D['canopy'] = self.process_parameters_D['canopy']

        else:

            self.record_D['canopy'] = 'uniform'

    def _Sample_name_parameters_ktima(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        # Set record parameters from the sample name
        sample_name = self.record_D['sample name']

        sample_name = sample_name.replace(' ','')

        sample_name_parts, self.record_D['subsample'] = sample_name.split('_')

        sample_name_parts = sample_name_parts.split('-')

        self.record_D['replicate'] = 0

        if len(sample_name_parts) == 5:
            
            self.record_D['replicate'] = int(sample_name_parts[3])

            sample_name_parts[3] = sample_name_parts[4]

        if sample_name_parts[3].lower() == 's':

            self.record_D['min_depth'], self.record_D['max_depth'] = '20','50'

        elif sample_name_parts[3].lower() == 't':

            self.record_D['min_depth'], self.record_D['max_depth'] = '0','20'

        else:

            msg = '❌  ERROR - depth code not recognised in sample name: %s' %(sample_name)

            print (msg)

            return None
        
        # TGTODO This is a derived parameter, check if reset later
        self.record_D['sample_id'] = sample_name_parts[1].replace(' ', '-').replace('_', '-').lower()
        
        if self.record_D['sample_id'][0] == '0':

            self.record_D['sample_id'] = self.record_D['sample_id'][1:]

        if sample_name_parts[2].lower() == 'ds':

            self.record_D['sample_preparation__name'] = 'ds'

        elif sample_name_parts[2].lower() == 'mx':

            self.record_D['sample_preparation__name'] = 'mx'

        elif sample_name_parts[2].lower() == 'no':

            self.record_D['sample_preparation__name'] = 'no'

        elif sample_name_parts[2].lower() == 'n0':

            self.record_D['sample_preparation__name'] == 'no'

        elif sample_name_parts[2].lower() == 'cu':

            self.record_D['sample_preparation__name'] = 'cu'

        else:

            msg = '❌  ERROR - sample preparation code not recognised in sample name: %s' %(sample_name)

            print (msg)
            
            return None

        self.record_D['point_id'] =  sample_name_parts[1].lower()

        self.record_D['sample_analysis_date'] = self.record_D['created at (utc)'][0:10].replace('-','')

        self.record_D['instrument_id'] = self.record_D['device id']

        return True 
    
    def _Sample_name_parameters_foulum(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

       # Set record parameters from the sample name
        sample_name = self.record_D['sample name']

        sample_name = sample_name.replace(' ','')

        sample_name_parts = sample_name.split('_')
 
        self.record_D['point_id'] = sample_name_parts[0].lower()

        self.record_D['min_depth'], self.record_D['max_depth'] = sample_name_parts[1].split('-')

        self.record_D['sample_analysis_date'] = self.record_D['created at (utc)'][0:10].replace('-','')

        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = sample_name_parts[2].lower()

        self.record_D['instrument_id'] = self.record_D['device id']

        self.record_D['sample_preparation__name'] = self.process_parameters_D['sample_preparation__name']

        return True
    
    def _Sample_name_parameters_neretva_ds(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

       # Set record parameters from the sample name
        sample_name = self.record_D['sample name']

        sample_name_parts = sample_name.split('_')
 
        self.record_D['point_id'] = '%s-%s' %(sample_name_parts[0][1:].lower(),sample_name_parts[0][0].lower())
    
        self.record_D['min_depth'], self.record_D['max_depth'] = sample_name_parts[1].split('-')

        self.record_D['sample_analysis_date'] = self.record_D['created at (utc)'][0:10].replace('-','')

        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = sample_name_parts[2].lower()

        self.record_D['instrument_id'] = self.record_D['device id']

        self.record_D['sample_preparation__name'] = self.process_parameters_D['sample_preparation__name']

        return True
    
    def _Sample_name_parameters_neretva_mx(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

       # Set record parameters from the sample name
        sample_name = self.record_D['sample name']

        sample_name_parts = sample_name.split('_')
 
        self.record_D['point_id'] = sample_name_parts[0].lower()
     
        self.record_D['min_depth'], self.record_D['max_depth'] = sample_name_parts[1].split('-')

        self.record_D['sample_analysis_date'] = self.record_D['created at (utc)'][0:10].replace('-','')

        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = sample_name_parts[2].lower()

        self.record_D['instrument_id'] = self.record_D['device id']

        self.record_D['sample_preparation__name'] = self.process_parameters_D['sample_preparation__name']

        return True
    
    def _Sample_name_parameters_boermarke_zeijen(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

       # Set record parameters from the sample name
        sample_name = self.record_D['sample name']

        sample_name_parts = sample_name.split('_')
 
        self.record_D['point_id'] = sample_name_parts[0].lower()

        self.record_D['min_depth'], self.record_D['max_depth'] = sample_name_parts[1].split('-')

        self.record_D['sample_analysis_date'] = self.record_D['created at (utc)'][0:10].replace('-','')

        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = sample_name_parts[2].lower()

        self.record_D['instrument_id'] = self.record_D['device id']

        self.record_D['sample_preparation__name'] = self.process_parameters_D['sample_preparation__name']

        return True

    def _Sample_name_parameters_loennstorp(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

       # Set record parameters from the sample name
        sample_name = self.record_D['sample name']

        sample_name_parts = sample_name.split('_')

        self.record_D['point_id'] = LOENNSTORP_POINT_ID_D[sample_name_parts[0].lower()]

        #self.record_D['point_id'] =  self.record_D['sample_id']

        self.record_D['min_depth'], self.record_D['max_depth'] = sample_name_parts[1].split('-')

        self.record_D['sample_analysis_date'] = self.record_D['created at (utc)'][0:10].replace('-','')

        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = sample_name_parts[2].lower()

        self.record_D['instrument_id'] = self.record_D['device id']

        self.record_D['sample_preparation__name'] = self.process_parameters_D['sample_preparation__name']

        return True
    
    def _Sample_name_parameters_jokioinen(self):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

       # Set record parameters from the sample name
        sample_name = self.record_D['sample name']

        sample_name_parts = sample_name.split('_')
 
        self.record_D['point_id'] = sample_name_parts[0].lower()

        #self.record_D['point_id'] =  self.record_D['sample_id']

        self.record_D['min_depth'], self.record_D['max_depth'] = sample_name_parts[1].split('-')

        self.record_D['sample_analysis_date'] = self.record_D['created at (utc)'][0:10].replace('-','')

        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = sample_name_parts[2].lower()

        self.record_D['instrument_id'] = self.record_D['device id']

        self.record_D['sample_preparation__name'] = self.process_parameters_D['sample_preparation__name']

        return True

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
        
        for item in COMPULSARY_DATA_RECORDS:

            if item not in self.record_D:

                if item in self.process_parameters_D:

                    self.record_D[item] = self.process_parameters_D[item]

                #elif item in self.inverse_parameter_D:
                    
                #    self.record_D[item] = self.record_D[self.inverse_parameter_D[item]]

                else:

                    print ('❌  ERROR - compulsory data not found: '+item)

                    print (' You can add <'+item+'> parameter to either:')

                    print('  - the method header definition file: %s' %(self.process_parameters.method_src_FPN))
                    
                    print('  - or the data file: %s' %(self.process_parameters.data_src_FPN))

                    return None

        return True

    def _Assemble_ossl_csv(self):

        self.record_D['license'] = 'only for use by AI4SH'
        self.record_D['doi'] = "not_yet_published"
        self.record_D['instrument_setting'] = "not recorded"
        self.record_D['depth'] = '%s-%s' %(self.record_D['min_depth'],self.record_D['max_depth'])
    
        #foss_csv_value_L = [self.record_D[item] for item in AI4SH_Key_L]
        neon_csv_value_L = [self.record_D[item] for item in AI4SH_Key_L]

        interpolated_ns_array = Interpolate_spectra(self.wavelength_L, self.record_D['value']['spectra'], 1350, 2550, 2, True)[0]

        neon_csv_value_L.extend(interpolated_ns_array.tolist())

        self.neon_values_L.append(neon_csv_value_L)

    def _Convert_spectra_to_OSSL(self):

        header_L = deepcopy(AI4SH_Key_L)

        for i in range(1350, 2551, 2):

            header_L.append("wl.%d" % (i))

        self._Write_OSSL_csv('ai4sh', header_L, self.neon_values_L)

def Process_neospectra_csv(project_FP,process, column_L, data_L_L, all_parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D, std_row = None):
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

    if hasattr(process.parameters, 'coordinates_FPN'):

        coordinate_D = Coordinates_fix(project_FP,process.parameters.coordinates_FPN)

    else:

        coordinate_D = None

    if process.overwrite:

        print('Overwrite is set to True, all existing JSON files will be deleted before processing new data')

        for item in ['ai4sh','xspectre','ossl']:

            dst_FP= '%s_%s' %(process.parameters.dst_FP,item)

            dst_FP = Full_path_locate(project_FP, dst_FP, True)

            Remove_path(dst_FP)
            
    # Initiate the json_db class
    json_db_C = json_db(project_FP,process, coordinate_D)

    json_db_C.equipment = process.parameters.procedure

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
        
    json_db_C._Get_wavelengths(column_L)

    # Add compulsary parameters with default values if they are not set in the csv data file
    json_db_C._Add_compulsary_default_parameters()

    json_db_C.neon_values_L = []

    # Loop all the data records in the csv data file
    for data_row in  data_L_L:

        # Convert the csv data row to a dict using the csv header records as keys
        json_db_C._Row_data_to_dict(data_row)

        # Rearrange the initial dictionary to hold the observations in a sub dictionary
        json_db_C._Rearrange_row_data_2_record()

        # Create a hierarchical dictionary to hold equipment -> methods (must be recreated in each loop)
        json_db_C._Set_equipment_method()

         # Direct the extraction of parameters from file name to the correct functionsite/function
        if process.parameters.pilot_site.lower() == 'ktima-gerovassiliou':
       
            result = json_db_C._Sample_name_parameters_ktima()

        elif process.parameters.pilot_site.lower() == 'foulum':
       
            result = json_db_C._Sample_name_parameters_foulum()

        elif process.parameters.pilot_site.lower() == 'jokioinen':
       
            result = json_db_C._Sample_name_parameters_jokioinen()

        elif process.parameters.pilot_site.lower() == 'neretva' and process.parameters.sample_preparation__name == 'ds':

            result = json_db_C._Sample_name_parameters_neretva_ds()

        elif process.parameters.pilot_site.lower() == 'neretva' and process.parameters.sample_preparation__name == 'mx':

            result = json_db_C._Sample_name_parameters_neretva_mx()

        elif process.parameters.pilot_site.lower() == 'boermarke-zeijen':

            result = json_db_C._Sample_name_parameters_boermarke_zeijen()

        elif process.parameters.pilot_site.lower() == 'loennstorp':

            result = json_db_C._Sample_name_parameters_loennstorp()

        else:
       
            print('❌  ERROR - pilot site not recognised: %s' %(process.parameters.pilot_site))
            
            return None

        if not result:

            msg = '❌  ERROR - problem setting parameters from sample name: %s' %(json_db_C.record_D['sample name'])
            
            print (msg)

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
        
        # Set the sampling log parameters
        json_db_C._Set_sampling_log()

        # Set sample id
        json_db_C._Set_sample_id()

        # Check the final record and set calculated parameters
        success = json_db_C._Check_set_final_record(project_FP)

        if not success:

            return None       

        # ===== Set the DB output objects =====

        # Set the db object point
        json_db_C._Set_point()

        # Set the db object site
        json_db_C._Set_site()

        # Set the db object data_soruce (= pilot for AI4SH)
        json_db_C._Set_data_source()

        # Attach the observation measurements to the equipment_method_D dictionary 
        json_db_C._Get_observation_measurements_xspectre()

        # Reset n_repeats to 3 (standard used for all neospectra measurements in AI4SH)
        json_db_C.record_D['n_repeats'] = 3

        # Set the observation metadata
        result = json_db_C._Set_observation_metadata()

        # Set the sample parameters
        json_db_C._Set_sample()

        # Assemble the complete sample event to a final dictionary containing all parameters
        sample_event_ai4sh = json_db_C._Assemble_sample_event_AI4SH_xspectre()

        if sample_event_ai4sh:

            # Dump the complete sample event to a JSON file
            json_db_C._Dump_sample_json(sample_event_ai4sh, 'ai4sh')

        else:

            print('❌ Error creating AI4SH JSON post')
        
        # Assemble the complete sample event to a final dictionary containing all parameters
        sample_event_xspectre = json_db_C._Assemble_sample_event_xspectre_xspectre()

        if sample_event_xspectre:

            # Dump the complete sample event to a JSON file
            json_db_C._Dump_sample_json(sample_event_xspectre, 'xspectre')

        else:

            print('❌ Error creating xspectre JSON post')

        json_db_C._Assemble_ossl_csv()

    sample_event_OSSL = json_db_C._Convert_spectra_to_OSSL()