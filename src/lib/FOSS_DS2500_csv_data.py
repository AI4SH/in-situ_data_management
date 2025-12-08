'''
Created on 14 jan 2025
Last updated on 25 aug 2025 (complete rewrite to object oriented class structure)
Updated on 1 Sept 2025 (doxygen comments added)
Updated 1 October - split out to common class for xspectre and ai4sh json structure

@author: thomasgumbricht
'''

from copy import deepcopy

import numpy as np

from scipy.special import exp10

# Package application imports
from src.lib import Coordinates_fix, Interpolate_spectra

from .common import common_json_db

# Default variables

LOENNSTORP_POINT_ID_D = {'4':'4-a','04':'4-a','5':'5-a','16':'16-a','20':'20-a','24':'24-a',
                         '51':'51-c','55':'55-c','60':'60-c','61':'61-c','72':'72-c',
                         '79':'79-d','80':'80-d','91':'91-d','95':'95-d','99':'99-d',
                         '1-sand':'1-sand','2-sand':'2-sand','3-organic':'3-organic',
                         '4-organic':'4-organic','5-organic':'5-organic','9-sand':'9-sand',
                         '001-sand':'1-sand','002-sand':'2-sand','003-organic':'3-organic',
                         '004-organic':'4-organic','005-organic':'5-organic','009-sand':'9-sand'}

OSSL_FOSA_Key_L = ["id.layer_local_c", # subsamle
                   "dataset.code_ascii_txt", # replicate
                   "id.layer_uuid_txt", # sample_id - links to other tables
                    "id.scan_local_c", #depth
                    "scan.visnir.date.begin_iso.8601_yyyy.mm.dd", # pilot site
                    "scan.visnir.date.end_iso.8601_yyyy.mm.dd", # analysis date
                    "scan.visnir.model.name_utf8_txt", # instrument model name
                    "scan.visnir.model.code_any_txt", # instruemnt id
                    "scan.visnir.method.optics_any_txt", # instruemnt setting
                    "scan.visnir.method.preparation_any_txt", # sample_preparation_name
                    "scan.visnir.license.title_ascii_txt", # license 1
                    "scan.visnir.license.address_idn_url", # license 2
                    "scan.visnir.doi_idf_url", # doi
                    "scan.visnir.contact.name_utf8_txt", # email
                    "scan.visnir.contact.email_ietf_txt"] # email

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

    def _Get_wavelengths(self,column_L):
        """
        @brief Extracts and processes wavelength data from the record dictionary.

        This function retrieves the 'wavelengths' entry from self.record_D, which is expected to be a comma-separated string of wavelength values.
        It splits this string into a list, converts each value to a float, and assigns the resulting list to self.wavelength_L. If the 'wavelengths'
        entry is missing or cannot be processed, an error message is printed and None is returned.

        @return True if successful, None if an error occurs.
        """
        self.wavelength_L = []

        for w in range(1,len(column_L)):

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

        self.record_D = dict(zip(self.column_L[0:1], row_data[0:1]))
   
        self.record_D['spectra'] = row_data[1:]

    def _Rearrange_row_data_2_record(self):

        self.indicator_L = ['spectra']

        self.indicator_D = {'spectra':'spectra'}

        self.record_D["value"] = {'spectra': self.record_D['spectra']}

        self.record_D["unit__name"] = {}

        for ind in self.indicator_L:
            
            self.record_D[ind] = [c.replace(',','.')for c in self.record_D[ind]] 

            try:

                self.record_D['value'][ind] = [float(c) for c in self.record_D[ind]]

            except (ValueError, TypeError):
                print ('❌ ERROR converting spectra to float')
                for i in self.record_D['spectra']:

                    print (i)
                    print (float(i) )

                self.record_D['value'][ind] = None

                return None

            # Convert the FOSS DS2500 absorbance to reflectance
            self.record_D['value'][ind] = 1/np.exp(np.array(self.record_D['value'][ind]))

            #self.record_D['value'][ind] = 1/exp10(np.array(self.record_D['value'][ind]))


            self.record_D['value'][ind] = self.record_D['value'][ind].tolist()

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

        return True

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
        sample_name = self.record_D['sample_id']

        print (sample_name)

        sample_name_parts = sample_name.split('_')

        top_or_sub = sample_name_parts[-1]

        if top_or_sub.lower() == 't':

            self.record_D['min_depth'], self.record_D['max_depth'] = '0','20'

        elif top_or_sub.lower() == 's':
        
            self.record_D['min_depth'], self.record_D['max_depth'] = '20','50'

        else:

            print ('urecognised totp/sub indicator in Foulum data: %s' %(sample_name))

            return None

        self.record_D['replicate'] =  sample_name_parts[-2].lower()

        self.record_D['sample_preparation__name'] = sample_name_parts[-3].lower()

        self.record_D['sample_id'] = '%s-%s' %(sample_name_parts[0].lower(), sample_name_parts[1].lower())
        
        self.record_D['point_id'] =  self.record_D['sample_id']

        self.record_D['subsample'] = 'a'

        return True
    
    def _Sample_name_parameters_neretva(self):
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
        sample_name = self.record_D['sample_id']

        print (sample_name)

        sample_name_parts = sample_name.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = sample_name_parts[2].split('-')

        self.record_D['replicate'] =  '0'

        #self.record_D['sample_preparation__name'] = sample_name_parts[-3].lower()

        self.record_D['sample_id'] = '%s-%s' %(sample_name_parts[1].lower(), sample_name_parts[0].lower())
        
        self.record_D['point_id'] =  self.record_D['sample_id']

        self.record_D['subsample'] = 'a'

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
        sample_name = self.record_D['sample_id']

        sample_name_parts = sample_name.split('_')

        if len(sample_name_parts) == 5:
 
            self.record_D['sample_id'] = sample_name_parts[1]

            if self.record_D['sample_id'][0] == '0':

                self.record_D['sample_id'] = self.record_D['sample_id'][1:] 

        elif len(sample_name_parts) == 6:
            
            self.record_D['sample_id'] = '%s-%s'  %(sample_name_parts[1],sample_name_parts[2])

            if self.record_D['sample_id'][0] == '0':

                self.record_D['sample_id'] = self.record_D['sample_id'][1:]

        elif len(sample_name_parts) == 7:

            self.record_D['sample_id'] = '%s-%s-%s'  %(sample_name_parts[1],sample_name_parts[2],sample_name_parts[3])

        self.record_D['point_id'] =  self.record_D['sample_id']
        print (sample_name)

        top_or_sub = sample_name_parts[-3]

        if top_or_sub.lower() == 'top':

            self.record_D['min_depth'], self.record_D['max_depth'] = '0','20'

        elif top_or_sub.lower() == 'sub':
        
            self.record_D['min_depth'], self.record_D['max_depth'] = '20','50'

        else:

            print ('urecognised totp/sub indicator in Foulum data: %s' %(sample_name))

            return None

        self.record_D['replicate'] =  sample_name_parts[-1].lower()

        self.record_D['sample_preparation__name'] = sample_name_parts[-2].lower()

        self.record_D['subsample'] = 'a'
        
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
        sample_name = self.record_D['sample_id']

        sample_name_parts = sample_name.split('_')

        if len(sample_name_parts) == 2:

            self.record_D['sample_id'] = LOENNSTORP_POINT_ID_D[sample_name_parts[0]]

        elif len(sample_name_parts) == 3:

            sample_id = '%s-%s' %(sample_name_parts[0],sample_name_parts[1].lower())

            self.record_D['sample_id'] = LOENNSTORP_POINT_ID_D[sample_id]

        self.record_D['point_id'] =  self.record_D['sample_id']

        top_or_sub = sample_name_parts[-1]

        if top_or_sub.lower() == 'top':

            self.record_D['min_depth'], self.record_D['max_depth'] = '0','20'

        elif top_or_sub.lower() == 'sub':
        
            self.record_D['min_depth'], self.record_D['max_depth'] = '20','50'

        else:

            print ('urecognised top/sub indicator in Foulum data: %s' %(sample_name))

            return None

        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = '0'

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
        sample_name = self.record_D['sample_id']

        print (sample_name)

        sample_name_parts = sample_name.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = sample_name_parts[2].split('-')

        self.record_D['replicate'] =  '0'

        self.record_D['sample_id'] = '%s-%s' %(sample_name_parts[0].lower(), sample_name_parts[1].lower())
        
        self.record_D['point_id'] =  self.record_D['sample_id']

        self.record_D['subsample'] = 'a'

        return True
       
    def _Assemble_foss_csv(self):

        self.record_D['license'] = 'only for use by AI4SH'

        self.record_D['doi'] = "not_yet_published"

        self.record_D['instrument_setting'] = "not recorded"

        self.record_D['depth'] = '%s-%s' %(self.record_D['min_depth'],self.record_D['max_depth'])

        ossl_csv_value_L = [self.record_D[item] for item in AI4SH_Key_L]

        interpolated_ns_array = Interpolate_spectra(self.wavelength_L, self.record_D['value']['spectra'], 400, 2500, 2, False)[0]

        if interpolated_ns_array is None:

            print(' ❌ ERROR - spectra interpolation failed for sample_id: %s' %(self.record_D['sample_id']))

            return None
        
        ossl_csv_value_L.extend(interpolated_ns_array.tolist())

        self.ossl_values_L.append(ossl_csv_value_L)

    def _Convert_spectra_to_OSSL(self):

        header_L = deepcopy(AI4SH_Key_L)

        for i in range(400, 2501, 2):

            header_L.append("wl.%d" % (i))

        self._Write_OSSL_csv('ai4sh', header_L, self.ossl_values_L)

def Process_ds2500_csv(project_FP,process, column_L, data_L_L, all_parameter_D, unit_D, method_D, equipment_D, equipment_model_D, equipment_id_D, std_row = None):
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

    coordinate_D = Coordinates_fix(project_FP,process.parameters.point_name_position_sampledate_FPN)

    if not coordinate_D:

        print ('❌  ERROR - reading the location, setting, sample date and coordinate csv file failed.')

        print('❌  File: %s' % (process.parameters.point_name_position_sampledate_FPN))

        return None
            
    # Initiate the json_db class
    #json_db_C = json_db(project_FP,process)
    json_db_C = json_db(project_FP,process, coordinate_D)

    json_db_C.equipment = process.parameters.procedure

    # Set and create the destination folder if it doesn't exist
    json_db_C._Set_dst_FP()

    # Clean and set the column headers
    json_db_C._Set_column_L(column_L)

    json_db_C._Get_wavelengths(column_L)

    # Add compulsary parameters with default values if they are not set in the csv data file
    json_db_C._Add_compulsary_default_parameters()

    json_db_C.ossl_values_L = []

    # Loop all the data records in the csv data file
    for data_row in  data_L_L:

        # Convert the csv data row to a dict using the csv header records as keys
        json_db_C._Row_data_to_dict(data_row)

        # Rearrange the initial dictionary to hold the observations in a sub dictionary
        success = json_db_C._Rearrange_row_data_2_record()

        if not success:

            print('❌  ERROR - could not arrange row of csv data: %s' %(json_db_C.record_D['sample_id']))
            
            return None

        # Create a hierarchical dictionary to hold equipment -> methods (must be recreated in each loop)
        json_db_C._Set_equipment_method()

         # Direct the extraction of parameters from file name to the correct functionsite/function
        if process.parameters.pilot_site.lower() == 'ktima-gerovassiliou':
       
            result = json_db_C._Sample_name_parameters_ktima()

        elif process.parameters.pilot_site.lower() == 'foulum':
       
            result = json_db_C._Sample_name_parameters_foulum()

        elif process.parameters.pilot_site.lower() == 'jokioinen':
       
            result = json_db_C._Sample_name_parameters_jokioinen()

        elif process.parameters.pilot_site.lower() == 'neretva':

            result = json_db_C._Sample_name_parameters_neretva()

        elif process.parameters.pilot_site.lower() == 'boermarke-zeijen':

            result = json_db_C._Sample_name_parameters_boermarke_zeijen()

        elif process.parameters.pilot_site.lower() == 'loennstorp':

            result = json_db_C._Sample_name_parameters_loennstorp()

        else:
       
            print('❌  ERROR - pilot site not recognised: %s' %(process.parameters.pilot_site))
            
            return None

        if not result:

            msg = '❌  ERROR - problem setting parameters from sample name: %s' %(json_db_C.record_D['sample_id'])
            
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

        # Attach the observation measurements to the equipment_method_D dictionary 
        json_db_C._Get_observation_measurements_xspectre()

        # Reset n_repeats to 3 (standard used for all neospectra measurements in AI4SH)
        json_db_C.record_D['n_repetitions'] = 3

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

        json_db_C._Assemble_foss_csv()

    #sample_event_OSSL = json_db_C._Convert_spectra_to_OSSL()