'''
Created on 17 September 2025

Updated 29 September 2025 (Refactored to inherit from common_json_db, removed redundant code, improved documentation)

Updated 1 October - split out to common class for xspectre and ai4sh json structure

@author: thomasgumbricht

Calculating spectral reflectance from the ration between a sample and a white reference

Sample: use per sample mean and standard deviation
sample_dark: use per sample mean and the sample standard deviation as standdard deviation

Whtite reference: use overall mean and standard deviation

'''

# Standard library imports
from os import path

from copy import deepcopy

# Thrid party imports
import numpy as np

# Package application imports
from .common import common_json_db

from src.utils import Full_path_locate, Remove_path

from src.lib import Coordinates_fix

# Default variables
COMPULSARY_DATA_RECORDS = ['pilot_country','pilot','pilot_site','point_id','min_depth','max_depth','sample_date',
                                'sample_preparation__name','subsample','replicate','sample_analysis_date','sample_preservation__name',
                                'sample_transport__name','transport_duration_h','sample_storage__name','user_analysis__email',
                                'user_sampling__email','user_logistic__email','instrument-brand__name', 'instrument-model__name',
                                'instrument_id','procedure']

PARAMETERS_WITH_ASSUMED_DEFAULT = ['sample_preparation__name','sample_preservation__name','sample_transport__name','sample_storage__name','transport_duration_h','replicate','subsample']

ASSUMED_DEFAULT_VALUES = [None,None,None,None,0,0,'a']

XSPECTRE_DEFAULT_VALUE_D = {'sample_preparation__name': ['sampling','prepcode'],
                            'pilot_site': ['locus'],
                            'prepcode': ['sampling','prepcode'],
                            'sample_preservation__name': None,
                            'sample_transport__name': None,
                            'sample_storage__name': None,
                            'transport_duration_h': 0,
                            'replicate': 0,
                            'subsample': 'a'
                           }

XSPECTRE_COMPULSARY_DATA_D = {'pilot_country': ['campaignshortid',1],
                                'pilot': ['campaignshortid',2],
                                'pilot_site': ['campaignshortid',2],
                                # 'sample_date': ['sampling','sampledate'],
                                'sample_preparation__name': ['sampling','prepcode'],
                                'sample_analysis_date': ['scandate'],
                                'sample_preservation__name': None,
                                'sample_transport__name': None,
                                'sample_storage__name': None,
                                'transport_duration_h': 0,
                                'replicate': 0,
                                'subsample': 'a'
                            }

REPLICATE_D = {0:0, '0':0, 1:1, 2:2, 3:3, 4:4, 5:5, 6:6, '1':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9, 
               'a': 0, 'b':1,'c':2,'d':3,'e':4,'f':5,'g':6,'h':7,'i':8,'j':9,'k':10,
                'ab':2,'ac':3} 

SUBSAMPLE_D = {'None':None,'a':"a",'b':"b",'c':"c",'d':"d",'e':"e",'f':"f",'g':"g",'h':"h",'i':"i",'j':"j",'k':"k",
               'a1':"a",'a2':"b",'a3':"c",'a4':"d",'b1':"e",'b2':"f",'b3':"g",
                  'c1':"h",'c2':"i",'c3':"j",'d1':"k",'d2':"l",'d3':"m"}

METHOD_D = {'npkphcth-s':"penetrometer",'ph-ise':"ise-ph",'ise-ph':"ise-ph"}

PREPCODE_D = {'field':'field','mx-lab':"mx-lab", 'in-situ':'field','h2o-iso':'mx-lab'}

LAB_ANALYSIS_METHOD_NAME_D = {'in-situ-TF38415':'ise-ph-soil','h2o-iso':'ise-ph-5xh2o','tds-iso':'tds-bipin-5xh2o','c12880ma':'diffuse reflectance spectroscopy',
                              'npkphcth-s':'penetrometer','TF38415':'ise-ph-5xh20','h2o-iso-TF38415':'ise-ph-5xh20'}

LOENNSTORP_POINT_ID_D = {'4':'4-a','04':'4-a','5':'5-a','16':'16-a','20':'20-a','24':'24-a',
                         '51':'51-c','55':'55-c','60':'60-c','61':'61-c','72':'72-c',
                         '79':'79-d','80':'80-d','91':'91-d','95':'95-d','99':'99-d'}
#def find_closest_epoch_date(epoch_L, K):

#    return epoch_L[min(range(len(epoch_L)), key = lambda i: abs(epoch_L[i]-K))]


def find_closest_epoch_dates_before_after(epoch_L, K):

    epoch_before_L = [x for x in epoch_L if x < K]

    epoch_after_L = [x for x in epoch_L if x > K]

    if not epoch_before_L:

        before = 0

    else:

        before = epoch_before_L[min(range(len(epoch_before_L)), key = lambda i: abs(epoch_before_L[i]-K))] if epoch_before_L else None

    if not epoch_after_L:

        after = 0

    else:

        after = epoch_after_L[min(range(len(epoch_after_L)), key = lambda i: abs(epoch_after_L[i]-K))] if epoch_after_L else None

    return before, after

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

        self.position_date_str_L = []

    def _Add_xspectre_compulsary_default_parameters_REDUNDANT(self, xspectre_json_D):
        """
        @brief Adds compulsory parameters with assumed default values to the process parameters dictionary if they are missing.

        This function checks for each parameter listed in PARAMETERS_WITH_ASSUMED_DEFAULT whether it exists in self.process_parameters_D.
        If a parameter is missing, it assigns the corresponding value from ASSUMED_DEFAULT_VALUES.

        @return None
        """
        '''
        if 'setting' in self.record_D:

            self.record_D['canopy'] = self.record_D['setting']

        elif 'canopy' in self.process_parameters_D:

            self.record_D['canopy'] = self.process_parameters_D['canopy']

        else:

            self.record_D['canopy'] = 'uniform'
        '''
        self.default_data_D = dict(zip(PARAMETERS_WITH_ASSUMED_DEFAULT, ASSUMED_DEFAULT_VALUES))

        for parameter in self.default_data_D:

            if type(XSPECTRE_DEFAULT_VALUE_D[parameter]) is list:

                try:
                    self.default_data_D[parameter] = xspectre_json_D[XSPECTRE_DEFAULT_VALUE_D[parameter][0]][XSPECTRE_DEFAULT_VALUE_D[parameter][1]]
                except:
                    self.default_data_D[parameter] = None

            else:

                self.default_data_D[parameter] = XSPECTRE_DEFAULT_VALUE_D[parameter]

    def _Check_set_xspectre_compulsary_parameters(self, xspectre_json_D):

        self.record_D = {}

        for item in COMPULSARY_DATA_RECORDS:

            if item in self.process_parameters_D:

                self.record_D[item] = self.process_parameters_D[item]

        for item in XSPECTRE_COMPULSARY_DATA_D:
            
            if type(XSPECTRE_COMPULSARY_DATA_D[item]) is list:

                if len(XSPECTRE_COMPULSARY_DATA_D[item]) == 1:

                    if item not in self.record_D:

                        self.record_D[item] = xspectre_json_D[XSPECTRE_COMPULSARY_DATA_D[item][0]]

                elif len(XSPECTRE_COMPULSARY_DATA_D[item]) == 2:

                    if type(XSPECTRE_COMPULSARY_DATA_D[item][1]) is int:

                        if item not in self.record_D:
                            
                            try:
                            
                                self.record_D[item] = xspectre_json_D[XSPECTRE_COMPULSARY_DATA_D[item][0]].split('_')[XSPECTRE_COMPULSARY_DATA_D[item][1]]
                            except:

                                self.record_D[item] = 'unknown'

                    elif item not in self.record_D:

                        self.record_D[item] = xspectre_json_D[XSPECTRE_COMPULSARY_DATA_D[item][0]][XSPECTRE_COMPULSARY_DATA_D[item][1]]

                elif len(XSPECTRE_COMPULSARY_DATA_D[item]) == 3:

                    if item not in self.record_D:

                        self.record_D[item] = xspectre_json_D[XSPECTRE_COMPULSARY_DATA_D[item][0]][XSPECTRE_COMPULSARY_DATA_D[item][1]][XSPECTRE_COMPULSARY_DATA_D[item][2]]
            
            else:

                self.record_D[item] = XSPECTRE_COMPULSARY_DATA_D[item]
         
        if 'sensor' in xspectre_json_D and isinstance(xspectre_json_D['sensor'], dict):

            self.equipment = list(xspectre_json_D['sensor'].keys())[0]

            self.record_D['n_repetitions'] = xspectre_json_D['sensor'][self.equipment]['samplerepeats']

            self.record_D['max_dn'] = xspectre_json_D['sensor'][self.equipment]['maxDN']

        elif 'sensor' in xspectre_json_D:

            self.equipment = xspectre_json_D['sensor']

            self.record_D['n_repetitions'] = xspectre_json_D[self.equipment]['samplerepeats']

            self.record_D['max_dn'] = xspectre_json_D[self.equipment]['maxDN']

        elif 'sensor' in xspectre_json_D['xparams']:

            self.equipment = xspectre_json_D['xparams']['sensor']

            self.record_D['n_repetitions'] = xspectre_json_D['xparams']['samplerepeats']

            self.record_D['max_dn'] = xspectre_json_D['xparams']['maxDN']

        else:

            self.equipment = xspectre_json_D['sensorid']

            self.record_D['n_repetitions'] =6

            self.record_D['max_dn'] = 850
        
        self.indicator_L = []

        self.record_D['unit__name'] ={}

        self.record_D['value'] ={}

        self.record_D['standard_deviation'] ={}

        # Spectra json do not have object <sensing>
        if 'sensing' in xspectre_json_D:

            for indicator_key in xspectre_json_D['sensing']:

                self.indicator_L.append(indicator_key)

                self.record_D['unit__name'][indicator_key] = xspectre_json_D['sensing'][indicator_key]['sensingunit']
                
                self.record_D['value'][indicator_key] = xspectre_json_D['sensing'][indicator_key]['mean']
                
                self.record_D['standard_deviation'][indicator_key] = xspectre_json_D['sensing'][indicator_key]['std'] if 'std' in xspectre_json_D['sensing'][indicator_key] else None
    
        return True
        
    def _FN_parameters_neretva_ise_ph(self, FPN):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        if not FN[0].isdigit():

            return None

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        #self.record_D['point_id'] =  FN_parts[0].replace(' ', '-').replace('_', '-').lower()


        #self.record_D['point_id'] =  '%s-%s' %(FN_parts[0][0].lower(), 
        #                                      FN_parts[0][1:].lower())
        self.record_D['point_id'] = '%s-%s' % (FN_parts[0][0:-1], FN_parts[0][-1].lower())
        
        self.record_D['instrument_id'] = '0'

        method, subsample = FN_parts[3].split('-')

        self.record_D['subsample'] = subsample

        self.record_D['replicate'] = 0

        self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[method]

        return True
    
    def _FN_parameters_neretva_gx16_ec(self, FPN):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """
        
        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        if not FN[0].isdigit():

            return None

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        #self.record_D['point_id'] =  FN_parts[0].replace(' ', '-').replace('_', '-').lower()

        #self.record_D['point_id'] =  '%s-%s' %(FN_parts[0][0].lower(), 
        #                                     FN_parts[0][1:].lower())

        self.record_D['point_id'] = '%s-%s' % (FN_parts[0][0:-1], FN_parts[0][-1].lower())

        self.record_D['instrument_id'] = '0'
        # 1R_0-20_tds-iso_tc_9520260_tds-gx16_tds_neretva_20241016_soil-raw
        method = FN_parts[2]

        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = 0

        self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[method]

        return True

    def _FN_parameters_neretva_penetrometer(self, FPN):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        #self.record_D['point_id'] =  FN_parts[0].replace(' ', '-').replace('_', '-').lower()
        #self.record_D['point_id'] =  '%s-%s' %(FN_parts[0][0].lower(), 
        #                                      FN_parts[0][1:].lower())
        self.record_D['point_id'] = '%s-%s' % (FN_parts[0][0:-1], FN_parts[0][-1].lower())
        
        self.record_D['instrument_id'] = FN_parts[2].split('-')[0].lower()

        subsample_replicate = FN_parts[2].split('-')[1].lower()

        if len(subsample_replicate) == 1:

            self.record_D['subsample'] = subsample_replicate

            self.record_D['replicate'] = 0

        elif len(subsample_replicate) == 2:

            self.record_D['subsample'] = subsample_replicate[0]
            try:
                self.record_D['replicate'] = int(subsample_replicate[1])-1
            except:

                if subsample_replicate[1].isdigit():
                    self.record_D['replicate'] = int(subsample_replicate[1])-1

                elif subsample_replicate[1] == ' ':

                    self.record_D['replicate'] = 0

                elif subsample_replicate[1] in REPLICATE_D:

                    self.record_D['replicate'] = REPLICATE_D[subsample_replicate[1]]

                else:

                    self.record_D['replicate'] = subsample_replicate[1]

        self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[FN_parts[3]]

        return True
            
    def _FN_parameters_boermarke_zeijen_penetrometer(self, FPN):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        self.record_D['point_id'] =  FN_parts[0][2:len(FN_parts[0])].replace(' ', '-').replace('_', '-').lower()

        if self.record_D['point_id'].startswith('0'):

            self.record_D['point_id'] = self.record_D['point_id'][1:]

        if 'extra' in self.record_D['point_id']:

            self.record_D['point_id'] = self.record_D['point_id'].replace('extra','-extra')

        elif 'xtra' in self.record_D['point_id']:

            self.record_D['point_id'] = self.record_D['point_id'].replace('xtra','-extra')

        #self.record_D['instrument_id'] = FN_parts[2].split('-')[0].lower()

        self.record_D['instrument_id'] = "0"

        if len(FN_parts) > 2 and '-' in FN_parts[2]:

            self.record_D['subsample'] = FN_parts[2].split('-')[1].lower()

        else:

            self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = 0

        self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[FN_parts[3]]

        return True
    
    def _FN_parameters_zazari_penetrometer(self, FPN):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """
        # 1_sub_a_npkphcth-s_sm-temp-ec-pH-npk_0002_zazari_20251020_soil-raw
        # Set record parameters from the file name
      
        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        if FN_parts[1] == 'top':

            self.record_D['min_depth'], self.record_D['max_depth'] = '0', '20'

        elif FN_parts[1] == 'sub':

            self.record_D['min_depth'], self.record_D['max_depth'] = '20', '50'

        elif FN_parts[1] == 'post-infiltration':

            self.record_D['min_depth'], self.record_D['max_depth'] = '0', '8'

            self.record_D['sample_preparation__name'] = 'post-infiltration'


        else:

            print ('❌  ERROR - depth interval not recognised in filename: %s' % (FN))

            return None

        if len(FN_parts[0]) == 1:

            self.record_D['point_id'] = '0%s' % (FN_parts[0])
        
        else:   

            self.record_D['point_id'] = FN_parts[0]

        if '-' in self.record_D['point_id']:

            self.record_D['point_id'], self.record_D['setting'] = self.record_D['point_id'].split('-')

            

        self.record_D['point_id'] = self.record_D['point_id'].replace(' ', '')
        
        self.record_D['instrument_id'] = FN_parts[5]

        self.record_D['subsample'] = FN_parts[2]

        self.record_D['replicate'] = 0

        try:
            self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[FN_parts[3]]
        except KeyError:
            print ('❌  ERROR - analysis method not recognised in filename: %s' % (FN))
            return None

        return True
    
    def _FN_parameters_jokioinen_penetrometer(self, FPN):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        #self.record_D['point_id'] =  FN_parts[0].replace(' ', '-').replace('_', '-').lower()
        self.record_D['point_id'] = '%s-%s' % (FN_parts[0][0:-1], FN_parts[0][-1].lower())

        self.record_D['instrument_id'] = FN_parts[2].split('-')[0].lower()

        if len(FN_parts) > 2 and '-' in FN_parts[2]:

            self.record_D['subsample'] = FN_parts[2].split('-')[1].lower()

        else:

            self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = 0

        self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[FN_parts[3]]

        return True
    
    def _FN_parameters_jokioinen_ise_ph(self, FPN):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        if FN.startswith('ref'):

            return None

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        #self.record_D['point_id'] =  FN_parts[0].replace(' ', '-').replace('_', '-').lower()

        #self.record_D['point_id'] =  '%s-%s' %(FN_parts[0][0].lower(), 
        #                                      FN_parts[0][1:].lower())
        
        self.record_D['point_id'] = '%s-%s' % (FN_parts[0][0:-1], FN_parts[0][-1].lower())

        
        #self.record_D['instrument_id'] = FN_parts[2].split('-')[0].lower()

        self.record_D['instrument_id'] = '0'

        method, subsample = FN_parts[3].split('-')

        method = '%s-%s' %(FN_parts[2], method)

        if len(FN_parts) > 2 and '-' in FN_parts[3]:

            self.record_D['subsample'] = subsample.lower()

        else:

            self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = 0

        self.record_D['sample_preparation__name'] = PREPCODE_D[FN_parts[2]]

        self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[method]

        if self.record_D['sample_preparation__name'] == 'field':
            self.record_D['instrument-brand__name'] = 'soil-ise-ph'

        return True 
    
    def _FN_parameters_jokioinen_gx16_ec(self, FPN):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """
        # 1R_0-20_tds-iso_tc_9520260_tds-gx16_tds_jokioinen_20241011_soil
        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        if not FN[0].isdigit():

            return None

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        #self.record_D['point_id'] =  FN_parts[0].replace(' ', '-').replace('_', '-').lower()

        #self.record_D['point_id'] =  '%s-%s' %(FN_parts[0][0].lower(), 
        #                                      FN_parts[0][1:].lower())
        self.record_D['point_id'] = '%s-%s' % (FN_parts[0][0:-1], FN_parts[0][-1].lower())

        self.record_D['instrument_id'] = '0'
            
        self.record_D['subsample'] = 'a'

        self.record_D['replicate'] = 0

        #self.record_D['sample_preparation__name'] = 'mx-lab'

        self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[FN_parts[2]]

        return True 
    
    def _FN_parameters_jokioinen_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """
        # 1R_0-20_tds-iso_tc_9520260_tds-gx16_tds_jokioinen_20241011_soil
        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        if not FN[0].isdigit():

            return None

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[2].split('-')

        self.record_D['point_id'] =  '%s-%s' %(FN_parts[0].lower(), 
                                              FN_parts[1].lower())

        #self.record_D['instrument_id'] = FN_parts[2].split('-')[0].lower()

        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']

        ''' 
        if len(FN_parts) > 2 and '-' in FN_parts[3]:

            self.record_D['subsample'] = FN_parts[3].split('-')[1].lower()

        else:
        '''
            
        self.record_D['subsample'] = FN_parts[3].lower()

        self.record_D['replicate'] = 0

        #self.record_D['sample_preparation__name'] = 'mx-lab'

        #self.record_D['analysis_method__name'] = LAB_ANALYSIS_METHOD_NAME_D[FN_parts[2]]

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        #self.record_D['instrument_id'] = FN_parts[2].split('-')[0].lower()

        # self.record_D['instrument_id'] = xspectre_json_D['sensorid']

        self.record_D['muzzle'] = xspectre_json_D['muzzleid']

        self.record_D['muzzle_id'] = 'X'

        self.record_D['sample_analysis_date'] = xspectre_json_D['scandate']

        return True 
    
    def _FN_parameters_neretva_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """
        # R9_20-50_B_c12880ma_raw-spectra_neretva_20241106_soil
        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        if FN[0] != 'R':

            return None

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        self.record_D['point_id'] =  '%s-%s' %(FN_parts[0][1:].lower(),
                                              FN_parts[0][0].lower())

        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']
            
        self.record_D['subsample'] = FN_parts[2].lower()

        self.record_D['replicate'] = 0

        #self.record_D['sample_preparation__name'] = 'mx-lab'

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        self.record_D['muzzle'] = xspectre_json_D['muzzleid']

        self.record_D['muzzle_id'] = 'X'

        self.record_D['form_factor'] = 'X'

        self.record_D['sample_analysis_date'] = xspectre_json_D['scandate']

        return True 
    
    def _FN_parameters_foulum_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        #256_C5_0-20_C_c12880ma_raw-spectra_foulum_20241106_soil.json

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        if not FN[0].isdigit():

            return None

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        if len(FN_parts[1]) == 1 and FN_parts[1][0].isdigit():  
            #366_2_C5_COMPSUB_12_A

            self.record_D['point_id'] =  '%s-%s-%s' %(FN_parts[0].lower(), 
                                                FN_parts[1].lower(),FN_parts[2].lower())  
            
            if FN_parts[3][0].isdigit():

                self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[3].split('-')

            elif FN_parts[3] == 'COMPTOP':

                self.record_D['min_depth'], self.record_D['max_depth'] = "0", "20"

            elif FN_parts[3] == 'COMPSUB':

                self.record_D['min_depth'], self.record_D['max_depth'] = "20", "50"

            else:

                return None

            self.record_D['subsample'] = FN_parts[5].lower()

        else:
            #256_C5_0-20_C_
            self.record_D['point_id'] =  '%s-%s' %(FN_parts[0].lower(), 
                                                FN_parts[1].lower())
  
            self.record_D['subsample'] = FN_parts[3].lower()

            if FN_parts[2][0].isdigit():

                self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[2].split('-')

            elif FN_parts[2] == 'COMPTOP':

                self.record_D['min_depth'], self.record_D['max_depth'] = "0", "20"

            elif FN_parts[2] == 'COMPSUB':

                self.record_D['min_depth'], self.record_D['max_depth'] = "20", "50"

            else:

                return None
            
        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']

        self.record_D['replicate'] = 0

        if 'DK-AI4SH-2024-MX' in FPN:
        
            self.record_D['sample_preparation__name'] = 'mx-lab'

        elif 'DK-AI4SH-2024-DS' in FPN:
        
            self.record_D['sample_preparation__name'] = 'ds'

        else:

            return None

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        self.record_D['muzzle'] = xspectre_json_D['muzzleid']

        self.record_D['muzzle_id'] = 'X'

        self.record_D['form_factor'] = 'X'

        self.record_D['sample_analysis_date'] = xspectre_json_D['scandate']

        return True 
    
    def _FN_parameters_loennstorp_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        #256_C5_0-20_C_c12880ma_raw-spectra_foulum_20241106_soil.json

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        if not FN[0].isdigit():

            return None

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        if FN_parts[1].lower() in ['sand','organic']:  
            # 001_SAND_0-20_B

            self.record_D['point_id'] =  '%s-%s' %(FN_parts[0][len(FN_parts[0])-1].lower(), 
                                                FN_parts[1].lower())  

            self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[2].split('-')

            self.record_D['subsample'] = FN_parts[3].lower()

        else:
            # 04_20-50_B_
            self.record_D['point_id'] =  LOENNSTORP_POINT_ID_D[FN_parts[0]]
   
            self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

            self.record_D['subsample'] = FN_parts[2].lower()
   
        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']

        self.record_D['replicate'] = 0
        
        #self.record_D['sample_preparation__name'] = 'ds'

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        self.record_D['muzzle'] = xspectre_json_D['muzzleid']

        self.record_D['muzzle_id'] = 'X'

        self.record_D['form_factor'] = 'X'

        self.record_D['sample_analysis_date'] = xspectre_json_D['scandate']

        return True 
    
    def _FN_parameters_munsoe_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        #256_C5_0-20_C_c12880ma_raw-spectra_foulum_20241106_soil.json

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        # different naming versions at different occassions
        if (len(FN_parts[0]) > 5):

            FN_parts = FN_parts[0].split('-')

        if not FN_parts[0][0].isdigit():

            FN_parts[0] = '%s%s' %(FN_parts[0][1:len(FN_parts[0])], FN_parts[0][0])
 
        self.record_D['point_id'] =  '%s-%s' %(FN_parts[0].lower(), 
                                            FN_parts[1].lower())  

        if FN_parts[2].lower() == 'd1':

            self.record_D['min_depth'], self.record_D['max_depth'] = "0", "20"

        elif FN_parts[2].lower() == 'd2':

            self.record_D['min_depth'], self.record_D['max_depth'] = "20", "50"
        
        else:

            return None
        
        if FN_parts[3].lower() in ['a', 'b', 'c']:

            self.record_D['replicate'] = FN_parts[3].lower()

        else:

            self.record_D['replicate'] = 'a'

        if 'sensor-serialnr' in xspectre_json_D:

            self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']

        else:

            self.record_D['instrument_id'] = self.process.parameters.instrument_id

        #self.record_D['replicate'] = 0
        
        #self.record_D['sample_preparation__name'] = 'ds'

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        self.record_D['muzzle'] = xspectre_json_D['muzzleid']

        self.record_D['muzzle_id'] = 'X'

        self.record_D['form_factor'] = 'X'

        self.record_D['sample_analysis_date'] = xspectre_json_D['scandate']

        return True
    
    def _FN_parameters_loennstorp_safe_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        #256_C5_0-20_C_c12880ma_raw-spectra_foulum_20241106_soil.json

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')
        # D_REF_4_0-20_A
        # D_REF_3_0-20_133_A

        try:
            
            self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[3].split('-') 
        
        except ValueError:
            
            print(" ❌ ERROR - invalid depth format in file name %s" % FN)
            
            return None

        self.record_D['subsample'] = 'a'
   
        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']

        if FN_parts[4].isdigit():

            self.record_D['point_id'] =  '%s-%s-%s_%s' %(FN_parts[0].lower(), 
                                            FN_parts[1].lower(),FN_parts[2].lower(),FN_parts[4])
            
            self.record_D['replicate'] = FN_parts[5].lower()

        else:

            self.record_D['point_id'] =  '%s-%s-%s' %(FN_parts[0].lower(), 
                                            FN_parts[1].lower(),FN_parts[2].lower())
            
            self.record_D['replicate'] = FN_parts[4].lower()
        
        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        self.record_D['muzzle'] = xspectre_json_D['muzzleid']

        self.record_D['muzzle_id'] = 'X'

        self.record_D['form_factor'] = 'X'

        self.record_D['sample_analysis_date'] = xspectre_json_D['scandate']

        return True
    
    def _FN_parameters_julita_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        if not 'sensor-serialnr' in xspectre_json_D:

            xspectre_json_D['sensor-serialnr'] = "unknown"

        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        items = FN_parts[0].split('-')

        if len(items) == 1:

            print ('ERROR - invalid point_id format in file name %s' % FN)

            return None

        if len(items) == 2:

            self.record_D['point_id'], self.record_D['replicate'] = items

        else:
            
            self.record_D['replicate'] = items[2]

            self.record_D['point_id'] = '%s-%s' %(items[0], items[1])

        self.record_D['point_id'] = self.record_D['point_id'].lower().replace('\u0308', '').replace('\u030a', '').replace('blomm', 'blom')
        
        self.record_D['min_depth'], self.record_D['max_depth'] = "0", "20"
         
        self.record_D['subsample'] = 'a'

        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']

        #self.record_D['sample_preparation__name'] = 'ds'

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        self.record_D['muzzle'] = xspectre_json_D['muzzleid']

        self.record_D['muzzle_id'] = 'X'

        self.record_D['form_factor'] = 'X'

        self.record_D['sample_analysis_date'] = xspectre_json_D['scandate']

        xspectre_json_D['sensor-serialnr'] = "get-via-wl"

        return True
    
    def _FN_parameters_tovetorp_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        if not 'sensor-serialnr' in xspectre_json_D:

            xspectre_json_D['sensor-serialnr'] = "unknown"

        FN = path.split(FPN)[1]

        print (FN)

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        items = FN_parts[0].split('-')

        self.record_D['point_id'] = items[0]
        
        self.record_D['min_depth'], self.record_D['max_depth'] = items[1],items[2]

        if len(items) > 3:

            self.record_D['replicate'] = items[3]

        else:
            
            self.record_D['replicate'] = '0'
         
        self.record_D['subsample'] = 'a'

        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']
        
        #self.record_D['sample_preparation__name'] = 'ds'

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        self.record_D['muzzle'] = xspectre_json_D['muzzleid']

        self.record_D['muzzle_id'] = 'X'

        self.record_D['form_factor'] = 'X'

        self.record_D['sample_analysis_date'] = xspectre_json_D['scandate']

        xspectre_json_D['sensor-serialnr'] = "get-via-wl"

        return True
    
    def _FN_parameters_boermarke_zeijen_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """

        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        self.record_D['min_depth'], self.record_D['max_depth'] = FN_parts[1].split('-')

        self.record_D['point_id'] =  FN_parts[0][2:].replace(' ', '-').replace('_', '-').lower()

        if self.record_D['point_id'].startswith('0'):

            self.record_D['point_id'] = self.record_D['point_id'][1:]

        if self.record_D['point_id'].endswith('extra'):

            self.record_D['point_id'] = '%s-extra' % self.record_D['point_id'].replace('extra', '')

        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']

        self.record_D['subsample'] = FN_parts[4].lower()

        self.record_D['replicate'] = 0

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        return True
    
    def _FN_parameters_zazari_spectra(self, FPN, xspectre_json_D):
        """
        @brief Ensures all compulsory record parameters are set in the row data dictionary.

        This function checks and sets compulsory parameters required for a data record. If the parameter
        cannot be found, an error message is printed with instructions for resolving the issue.

        @details
        - Iterates through COMPULSARY_DATA_RECORDS to ensure each is present and non-empty.
        - Prints error and instructions if a compulsory parameter cannot be found.

        @return True if all compulsory parameters are set successfully, None if any are missing and cannot be resolved.
        """
        # 02_sub_a_mx_c14384ma-01_raw-spectra_zazari_20251022_soil
        # Set record parameters from the file name
        FN = path.split(FPN)[1]

        FN =  path.splitext(FN)[0]

        FN_parts = FN.split('_')

        if FN_parts[1] == 'top' :
            
            self.record_D['min_depth'], self.record_D['max_depth'] = "0", "20"

        elif FN_parts[1] == 'sub' :
            self.record_D['min_depth'], self.record_D['max_depth'] = "20", "50"

        else:

            print ('ERROR - invalid depth format for Zazari in file name %s' % FN)

            return None

        self.record_D['point_id'] =  FN_parts[0].replace(' ', '-').replace('_', '-').lower()

        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']

        self.record_D['subsample'] = FN_parts[2].lower()

        self.record_D['replicate'] = 0

        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'

        return True
    
    def _Get_spectra_white_reference(self, spectra_epoch_time, white_reference_D):

        # TGTODO - check that it is the same instrument and scan tuning
        white_reference_epoch_L = list(white_reference_D.keys())

        before, after = find_closest_epoch_dates_before_after(white_reference_epoch_L, spectra_epoch_time)

        if before and after:
           
            pass
            #def divide(x): return x / 2

            #L = [white_reference_D[before]['value'], white_reference_D[after]['value']]

            #white_reference_value = list(map(divide, map(sum, zip(*L))))

            #L = [white_reference_D[before]['standard_deviation'], white_reference_D[after]['standard_deviation']]

            #white_reference_standard_deviation = list(map(divide, map(sum, zip(*L))))

        elif len(white_reference_epoch_L) > 1:
            # Get the average of all white references if there are more than one

            L = []

            for key in white_reference_D:

                L.append(white_reference_D[key]['value_A'])

            A = np.array(L)

            mean_A = np.mean(np.array(A), axis=0)

            var_A = np.std(np.array(A), axis=0) * np.std(np.array(A), axis=0)

            total_samlple_size = len(L)*3

            within_group_sum_of_squares = (len(L)-1) * var_A

            between_group_sum_of_squares = sum((mean_A - np.mean(A, axis=0)) * (mean_A - np.mean(A, axis=0))) * len(L)

            pooled_variance_A = (within_group_sum_of_squares + between_group_sum_of_squares) / (total_samlple_size - 1)

            overall_standard_deviation_A = np.sqrt(pooled_variance_A)

            pooled_mean_A = mean_A

            white_reference_value_A = white_reference_D[white_reference_epoch_L[0]]['value_A']

            white_reference_value_standard_deviation_A = white_reference_D[white_reference_epoch_L[0]]['standard_deviation_A']
      
        elif before:

            white_reference_value_A = white_reference_D[before]['value_A']

            white_reference_value_standard_deviation_A = white_reference_D[before]['standard_deviation_A']

        elif after:

            white_reference_value_A = white_reference_D[after]['value_A']

            white_reference_value_standard_deviation_A = white_reference_D[after]['standard_deviation_A']

        else:

            return None
        
        self.white_reference_value_A = white_reference_value_A

        self.white_reference_value_standard_deviation_A = white_reference_value_standard_deviation_A

        return True
    
    def _Calculate_spectra_reflectance(self):

        # global overall_white_reference_value_mean_A, overall_white_reference_value_standard_deviation_A
        # global overall_white_reference_dark_mean_A, overall_white_reference_dark_standard_deviation_A

        self.record_D['sample_reflectance_A'] = white_reference_reflectance_A = white_reference_reflectance_A = overall_white_reference_value_mean_A - overall_white_reference_dark_mean_A

        self.record_D['sample_reflectance_A'] = sample_reflectance_A = self.record_D['value_A'] - self.record_D['dark_A']

        #self.record_D['sample_reflectance_A'] = sample_reflectance_A

        #self.record_D['white_reference_reflectance_A'] = white_reference_reflectance_A

        # Calculate the average spectra reflectance as fraction of white reflectance with dark correction
        #self.record_D['reflectance_value_A'] = (self.record_D['value_A'] - self.record_D['dark_value_A']) / \
        #(self.white_reference_value_A - white_reference_dark_value_A)

        self.record_D['reflectance_value_A'] = sample_reflectance_A / white_reference_reflectance_A 

        #white_reference_reflectance_A = self.white_reference_value_A - self.white_reference_dark_A
        #white_reference_reflectance_A = self.white_reference_value_A - white_reference_dark_value_A
        
        #self.record_D['reflectance_value_A'] = reflectance_value_A = self.record_D['value_A'] - self.record_D['dark_value_A']

        # See https://www.statisticshowto.com/statistics-basics/error-propagation/#addition
        white_reference_standard_deviation_A = np.sqrt(overall_white_reference_value_standard_deviation_A + overall_white_reference_dark_standard_deviation_A)

        sample_standard_deviation_A = np.sqrt(self.record_D['value_standard_deviation_A'] + self.record_D['dark_standard_deviation_A'] )

        # See https://stats.stackexchange.com/questions/363737/error-propagation-in-dividing-the-averages-of-two-data-sets
        self.record_D['reflectance_standard_deviation_A'] = np.sqrt( (white_reference_standard_deviation_A/white_reference_reflectance_A) *\
                                                    (white_reference_standard_deviation_A/white_reference_reflectance_A) +\
                                                    (sample_standard_deviation_A/sample_reflectance_A ) *\
                                                    (sample_standard_deviation_A/sample_reflectance_A ) )

        self.record_D['reflectance_standard_deviation_A'] *= sample_reflectance_A/white_reference_reflectance_A
   
    def _Set_spectra_record(self, xspectre_json_D):

        if not 'sensor-serialnr' in xspectre_json_D:

            xspectre_json_D['sensor-serialnr'] = "unknown"

        if not self.equipment in xspectre_json_D:

            xspectre_json_D[self.equipment] = {}

            xspectre_json_D[self.equipment]['samplerepeats'] = 6
            xspectre_json_D[self.equipment]['darkrepeats'] = 2

        if 'darkStd' in xspectre_json_D:
            
            xspectre_json_D['darkstd'] = xspectre_json_D['darkStd']

        self.record_D['value_A'] = np.array(xspectre_json_D['samplemean']) 
        self.record_D['value_standard_deviation_A'] = np.array(xspectre_json_D['samplestd']) 
        self.record_D['dark_A'] = np.array(xspectre_json_D['darkmean'])
        self.record_D['dark_standard_deviation_A'] = np.array(xspectre_json_D['darkstd'])
        self.record_D['n_repeats'] = xspectre_json_D[self.equipment]['samplerepeats']
        self.record_D['n_dark_repeats'] = xspectre_json_D[self.equipment]['darkrepeats']
        self.record_D['unit__name'] = 'reflectance'
        self.record_D['indicator__name'] =  'reflectance'
        self.record_D['analysis_method__name'] = 'diffuse reflectance spectroscopy'       
        self.record_D['instrument_id'] = xspectre_json_D['sensor-serialnr']
        self.record_D['instrument-brand__name'] = self.record_D['instrument-brand__name']
        self.record_D['instrument-model__name'] = self.record_D['instrument-model__name']
        self.record_D['storage_duration_h'] = 0

    def _Get_xspectre_spectra_measurements(self, xspectre_json_D):
        """
        @brief Extracts spectra measurements from a data row and appends them to the equipment-method mapping.

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

        # Get number of missing values, below zero, over unity, and saturated values
        n_values_below_zero = (self.record_D['reflectance_value_A'] < 0.0).sum()

        if n_values_below_zero > 0:

            self.record_D['reflectance_standard_deviation_A'][self.record_D['reflectance_value_A'] < 0.0] = -9999

            self.record_D['reflectance_value_A'][self.record_D['reflectance_value_A'] < 0.0] = -9999

        n_values_above_unity = (self.record_D['reflectance_value_A'] > 1.0).sum()

        if n_values_above_unity > 0:

            self.record_D['reflectance_standard_deviation_A'][self.record_D['reflectance_value_A'] > 1.0] = -9999

            self.record_D['reflectance_value_A'][self.record_D['reflectance_value_A'] > 1.0] = -9999

        n_values_too_varying = (self.record_D['reflectance_standard_deviation_A'] > 1.0).sum()

        if n_values_too_varying > 0:

            self.record_D['reflectance_standard_deviation_A'][self.record_D['reflectance_standard_deviation_A'] > 1.0] = -9999

            self.record_D['reflectance_value_A'][self.record_D['reflectance_standard_deviation_A'] > 1.0] = -9999

        n_missing_values = np.isnan(self.record_D['reflectance_value_A']).sum()

        if n_missing_values > 0:

            self.record_D['reflectance_value_A'][np.isnan(self.record_D['reflectance_value_A'])] = -9999

            self.record_D['reflectance_standard_deviation_A'][np.isnan(self.record_D['reflectance_value_A'])] = -9999

        n_saturated_values = (self.record_D['value_A'] > self.record_D['max_dn']).sum()
        
        if n_saturated_values > 0:

            self.record_D['reflectance_value_A'][self.record_D['value_A'] > self.record_D['max_dn']] = -9999
            
            self.record_D['reflectance_standard_deviation_A'][self.record_D['value_A'] > self.record_D['max_dn']] = -9999   

        n_saturated_reference_values = (white_reference_max_A > self.record_D['max_dn']).sum()

        if n_saturated_reference_values > 0:

            self.record_D['reflectance_value_A'][white_reference_max_A > self.record_D['max_dn']] = -9999
            
            self.record_D['reflectance_standard_deviation_A'][white_reference_max_A > self.record_D['max_dn']] = -9999

        

        # Savve error statistics
        error_D = {'n_value_below_zero': int(n_values_below_zero),
                   'n_value_above_unity': int(n_values_above_unity),
                   'n_value_too_varying': int(n_values_too_varying),
                   'n_missing_value': int(n_missing_values),
                   'n_saturated_value': int(n_saturated_values),
                   'n_saturated_reference_value': int(n_saturated_reference_values)
                     }
        
        # Some parameters that are lacking in earlier versions

        if not 'headtrail' in xspectre_json_D[self.equipment]:
            xspectre_json_D[self.equipment]['headtrail'] = 0

        if not 'parameters' in xspectre_json_D[self.equipment]:
            xspectre_json_D[self.equipment]['parameters'] = {}
            xspectre_json_D[self.equipment]['parameters']['LED_mset_mV'] = xspectre_json_D['xparams']['voltage']
            xspectre_json_D[self.equipment]['parameters']['stabilistaiontime'] = xspectre_json_D['xparams']['stabilisationtime']
            xspectre_json_D[self.equipment]['parameters']['scantuning'] = {}
            xspectre_json_D[self.equipment]['parameters']['scantuning']['nIntegrationTimes'] = 1
            xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes'] = {}
            if 'c12880' in self.equipment:
                
                xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes']['0'] = [xspectre_json_D['xparams']['integrationtime'], 0, 288]    
            
            else:
                
                xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes']['0'] = [xspectre_json_D['xparams']['integrationtime'], 0, 256]

        if 'samplestd' in xspectre_json_D:

            observation_D =  {'value': self.record_D['reflectance_value_A'].tolist(), 
                        'standard_deviation': self.record_D['reflectance_standard_deviation_A'].tolist(), 
                        #'n_repeats': self.record_D['n_repeats'],
                        'unit__name': self.record_D['unit__name'],
                        'indicator__name':  self.record_D['indicator__name'],
                        'procedure': self.record_D['procedure'],
                        'analysis_method__name': self.record_D['analysis_method__name'], 
                        'instrument-brand__name': self.record_D['instrument-brand__name'],
                        'instrument-model__name': self.record_D['instrument-model__name'],
                        'instrument_id': self.record_D['instrument_id']}
        else:

            observation_D =  {'value': self.record_D['reflectance_value_A'].tolist(), 
                        #'n_repeats': self.record_D['n_repeats'],
                        'unit__name': self.record_D['unit__name'],
                        'indicator__name':  'reflectance',
                        'procedure': self.record_D['procedure'],
                        'analysis_method__name': self.record_D['analysis_method__name'],
                        'instrument-brand__name': self.record_D['instrument-brand__name'],
                        'instrument-model__name': self.record_D['instrument-_model__name'],
                        'instrument_id': self.record_D['instrument_id']}

        spectra_scan_tuning_D =  {
                        'dark_repeat': xspectre_json_D[self.equipment]['darkrepeats'],
                        'head_trail_repeat': xspectre_json_D[self.equipment]['headtrail'],
                        'led_mv': xspectre_json_D[self.equipment]['parameters']['LED_mset_mV'],
                        'stabilisation_time_ms': xspectre_json_D[self.equipment]['parameters']['stabilistaiontime'],
                        
                        }
        
        self.original_scan_dn_D = {'sample_mean': xspectre_json_D['samplemean'],
                            'sample_standard_deviation': xspectre_json_D['samplestd'],
                            'dark_mean': xspectre_json_D['darkmean']
                            }
        
        if xspectre_json_D[self.equipment]['darkrepeats'] > 1:

            self.original_scan_dn_D['dark_standard_deviation'] = xspectre_json_D['darkstd']
        
        self.white_reference_D ={"white_reference":white_reference_FN_L}
        
        self.equipment_method_D[self.equipment].append(observation_D)

        # From here the metadata is only for xspectre

        #obs_D = deepcopy(observation_D)

        scan_tuning__code = str(xspectre_json_D[self.equipment]['parameters']['scantuning']['nIntegrationTimes'])

        for i in range(xspectre_json_D[self.equipment]['parameters']['scantuning']['nIntegrationTimes']):

            scan_tuning__code += '_%s-%s-%s' % (xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes'][str(i)][0],
                                            xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes'][str(i)][1],
                                            xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes'][str(i)][2])
   
        spectra_scan_tuning_D['scan_tuning__code'] = scan_tuning__code
    
        self.xspectre_spectra_meta_D = {'spectra_scan_tuning':spectra_scan_tuning_D,
                                        'error': error_D} #['spectra_scan_tuning'] = spectra_scan_tuning_D
        
        self.xspectre_spectra_meta_D['muzzle'] = {"code":xspectre_json_D['muzzleid']}
        #self.record_D['muzzle_id'] = obs_D['muzzle_id'] = 0
        if not 'formfactor' in xspectre_json_D:
            self.xspectre_spectra_meta_D['muzzle']['formfactor'] = self.process.parameters.muzzle_formfactor
        else:
            self.xspectre_spectra_meta_D['muzzle']['formfactor'] = xspectre_json_D['formfactor']



        #obs_D['error'] = error_D

        #self.xspectre_method_D = {self.equipment: []}

        #self.xspectre_method_D[self.equipment].append(obs_D)

    def _Xspectre_sample_dates(self, json_FPN):
        """
        @brief Retrieves sample dates from xspectre JSON data v089 files.

        This function reads data JSON files with both data and metadata, validates their contents,
        extracts sample dates, and compiles them into a summary.

        @param process An object containing parameters and file paths for method and data CSV files.
        @return None. Prints error messages if files or parameters are invalid.
        """

        json_FN = path.split(json_FPN)[1]

        json_FN_core = path.splitext(json_FN)[0]

        print('Processing:', json_FN_core)

        FN_parts = json_FN_core.split('_')

        self.record_D['sample_date'] = FN_parts[len(FN_parts)-2]
    
def Extract_xspectre_json_v089(project_FP, process, json_FPN, white_reference_D, coordinate_D):
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

    from src.utils import Read_json

    # Initiate the json_db class
    json_db_C = json_db(project_FP,process, coordinate_D)

    # Create the destination folder if it doesn't exist
    json_db_C._Set_dst_FP()

    # Read the json data file
    xspectre_json_D = Read_json(json_FPN)

    # Check that all compulsory parameters are set in the xspectre json
    result = json_db_C._Check_set_xspectre_compulsary_parameters(xspectre_json_D)

    if not result:

        return None
    
    # Create a hierarchical dictionary to hold equipment -> methods (must be recreated in each loop)
    json_db_C._Set_equipment_method()

    # ===== Direct the extraction of parameters from file name to the correct functionsite/function =====

    if process.parameters.pilot_site.lower() == 'neretva' and\
         process.parameters.procedure == 'xspectre-ise-ph':
       
        result = json_db_C._FN_parameters_neretva_ise_ph(json_FPN)

    elif process.parameters.pilot_site.lower() == 'neretva' and\
         process.parameters.procedure == 'xspectre-gx16-ec':
       
        result = json_db_C._FN_parameters_neretva_gx16_ec(json_FPN)

    elif process.parameters.pilot_site.lower() == 'neretva' and\
         process.parameters.procedure == 'xspectre-penetrometer':
       
        result = json_db_C._FN_parameters_neretva_penetrometer(json_FPN)

    elif process.parameters.pilot_site.lower() == 'boermarke-zeijen' and\
         process.parameters.procedure == 'xspectre-penetrometer':

        result = json_db_C._FN_parameters_boermarke_zeijen_penetrometer(json_FPN)

    elif process.parameters.pilot_site.lower() == 'jokioinen' and\
         process.parameters.procedure == 'xspectre-ise-ph':

        result = json_db_C._FN_parameters_jokioinen_ise_ph(json_FPN)

    elif process.parameters.pilot_site.lower() == 'jokioinen' and\
         process.parameters.procedure == 'xspectre-penetrometer':

        result = json_db_C._FN_parameters_jokioinen_penetrometer(json_FPN)

    elif process.parameters.pilot_site.lower() == 'jokioinen' and\
         process.parameters.procedure == 'xspectre-gx16-ec':

        result = json_db_C._FN_parameters_jokioinen_gx16_ec(json_FPN)

    elif process.parameters.pilot_site.lower() == 'jokioinen' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_jokioinen_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'neretva' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_neretva_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'foulum' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_foulum_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'loennstorp' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_loennstorp_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'munsoe' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_munsoe_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'loennstorp_safe' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_loennstorp_safe_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'julita' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_julita_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'tovetorp' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_tovetorp_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'boermarke-zeijen' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_boermarke_zeijen_spectra(json_FPN,xspectre_json_D)

    elif process.parameters.pilot_site.lower() == 'zazari' and\
         process.parameters.procedure == 'xspectre-penetrometer':
        
        result = json_db_C._FN_parameters_zazari_penetrometer(json_FPN)

    elif process.parameters.pilot_site.lower() == 'zazari' and\
         process.parameters.procedure == 'xspectre-spectra':

        result = json_db_C._FN_parameters_zazari_spectra(json_FPN,xspectre_json_D)

    else:

        print('❌  ERROR - pilot site observation not recognised: %s, %s' % (process.parameters.pilot_site, process.parameters.procedure))

        return None

    if not result:

        print('❌  ERROR - setting record parameters from file name failed: %s' % (path.split(json_FPN))[1])

        return None
    
    # ===== Sepcial handling for creating csv file of locus, samle data and corrdinates =====
    if process.parameters.procedure == 'xspectre-penetrometer':

        pass

        #json_db_C._Xspectre_sample_dates(json_FPN)

    # ===== Reorganise all inpit data into a single record dictionary =====

    # Set the location (locus) sample date and coordinate parameters
    # These must be given in a separate csv file
    result = json_db_C._Set_locus_sample_date_coordinate()

    if not result:

        return None
    
    # Add compulsary parameters with default values if they are not in the source data
    # json_db_C._Add_xspectre_compulsary_default_parameters(xspectre_json_D)
    # Check and set the final record parameters
    result = json_db_C._Check_set_final_record(json_FPN)

    if not result:

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
    
    if not result:

        return None
    
    # Set the sample parameters
    json_db_C._Set_sample()

    # Set the observed measurements linked to the correct equipment and method
    if process.parameters.procedure == 'xspectre-spectra':
        #TGTODO Because there is no timestamp I can can resolve whihc whiteref to use for each sample
        #instead I use all whiteref from each sampling log, must be updated
        #json_db_C._Get_spectra_white_reference(path.getctime(json_FPN), white_reference_D)

        json_db_C._Set_spectra_record(xspectre_json_D)

        json_db_C._Calculate_spectra_reflectance()
    
        json_db_C._Get_xspectre_spectra_measurements(xspectre_json_D)

    else:

        json_db_C._Get_observation_measurements_xspectre()

    # ===== Assemble and write the complete DB record =====

    # Assemble the complete sample event in AI4SH format 
    sample_event_ai4sh = json_db_C._Assemble_sample_event_AI4SH_xspectre()

    if sample_event_ai4sh:

        # Dump the complete sample event to a JSON file
        json_db_C._Dump_sample_json(sample_event_ai4sh, 'ai4sh')

    else:

        print('❌ Error creating AI4SH JSON post from xspectre data')
    
    # Assemble the complete sample event in xspectre format
    sample_event_xspectre = json_db_C._Assemble_sample_event_xspectre_xspectre()

    if sample_event_xspectre:

        # Dump the complete sample event to a JSON file
        json_db_C._Dump_sample_json(sample_event_xspectre, 'xspectre')

    else:

        print('❌ Error creating xspectre JSON post from xspectre data')

    if process.parameters.procedure == 'xspectre-penetrometer':

        pass
        '''# Write the position date file
        json_db_C._Xspectre_sample_dates(json_FPN)

        position_date_name = '%s-%s' %(json_db_C.record_D['position_date']['position_name'],json_db_C.record_D['position_date']['sample_date'])

        if not position_date_name in position_date_str_L:

            position_date_str_L.append(position_date_name)

            items = list(json_db_C.record_D['position_date'].values())

            items_str = ",".join(items)

            position_date_F.write(items_str + "\n")

        '''
    
def Extract_white_reference_json_v089(project_FP, process, json_FPN):
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

    from src.utils import Read_json

    # Initiate the json_db class
    json_db_C = json_db(project_FP,process, False)

    # Read the json data file
    xspectre_json_D = Read_json(json_FPN)

    # Whitereference Check that all compulsory parameters are set in the xspectre json
    result = json_db_C._Check_set_xspectre_compulsary_parameters(xspectre_json_D)

    if not result:

        return None
    
    # Create a hierarchical dictionary to hold equipment -> methods (must be recreated in each loop)
    json_db_C._Set_equipment_method()

    json_db_C._Set_spectra_record(xspectre_json_D)

    # Set the observation metadata
    result = json_db_C._Set_observation_metadata()
    
    if not result:

        return None
    
    return json_db_C.record_D

def Calculate_white_reference_statistics(white_reference_value_L, 
                                             white_reference_value_standard_deviation_L, 
                                             white_reference_value_n_scans_L,
                                             white_reference_dark_L,
                                             white_reference_dark_standard_deviation_L,
                                             white_reference_dark_n_scans_L):
    """
    @brief Calculates the standard deviation of white reference dark values.

    This function takes a list of white reference dark value arrays, computes the standard deviation across them,
    and prints the resulting standard deviation array. It is used to assess the variability in dark measurements
    from multiple white reference scans.

    @param white_reference_dark_L List of arrays, each representing dark values from a white reference scan.
    @return None. Prints the calculated standard deviation array.
    """

    global overall_white_reference_value_mean_A, overall_white_reference_value_standard_deviation_A
    global overall_white_reference_dark_mean_A, overall_white_reference_dark_standard_deviation_A
    global white_reference_max_A

    white_reference_value_n_scans_A = np.array(white_reference_value_n_scans_L)
    white_reference_value_standard_deviation_A = np.array(white_reference_value_standard_deviation_L)
    white_reference_value_A = np.array(white_reference_value_L)
    white_reference_dark_n_scans_A = np.array(white_reference_dark_n_scans_L)
    white_reference_dark_standard_deviation_A = np.array(white_reference_dark_standard_deviation_L)
    white_reference_dark_A = np.array(white_reference_dark_L)
    
    # overall value mean
    overall_white_reference_value_mean_A = np.mean(white_reference_value_A ,axis=0)

    # overall value standard deviation
    if np.sum(white_reference_value_n_scans_A) <= white_reference_value_n_scans_A.shape[0]:

        overall_white_reference_value_standard_deviation_A = np.std(white_reference_value_A,axis=0)

    else:
        white_reference_squared_sum_value_std_A = white_reference_value_standard_deviation_A ** 2 + white_reference_value_A ** 2
        
        white_reference_value_squared_xn_A = white_reference_value_n_scans_A[:, np.newaxis] * white_reference_squared_sum_value_std_A      
        
        white_reference_value_squared_xn_sum_A = np.sum(white_reference_value_squared_xn_A, axis=0)
        
        white_reference_value_variance_A = white_reference_value_squared_xn_sum_A / np.sum(white_reference_value_n_scans_A) - overall_white_reference_value_mean_A ** 2

        overall_white_reference_value_standard_deviation_A = np.sqrt(white_reference_value_variance_A)

    # overall dark mean
    overall_white_reference_dark_mean_A = np.mean(white_reference_dark_A ,axis=0)

    # overall dark standard deviation
    if np.sum(white_reference_dark_n_scans_A) <= white_reference_dark_n_scans_A.shape[0]:

        overall_white_reference_dark_standard_deviation_A = np.std(white_reference_dark_A,axis=0)

    else:
    
        white_reference_squared_sum_dark_std_A = white_reference_dark_standard_deviation_A ** 2 + white_reference_dark_A ** 2
        
        white_reference_dark_squared_xn_A = white_reference_dark_n_scans_A[:, np.newaxis] * white_reference_squared_sum_dark_std_A      
        
        white_reference_dark_squared_xn_sum_A = np.sum(white_reference_dark_squared_xn_A, axis=0)
        
        white_reference_dark_variance_A = white_reference_dark_squared_xn_sum_A / np.sum(white_reference_dark_n_scans_A) - overall_white_reference_dark_mean_A ** 2

        overall_white_reference_dark_standard_deviation_A = np.sqrt(white_reference_dark_variance_A)

    # Extract the maximum white reference value for each wavelength

    white_reference_max_A = white_reference_value_A.max(axis=0)

    #print ('Overall white reference value mean:', overall_white_reference_value_mean_A)

def Get_all_white_reference_data(project_FP, process, json_FPN_L):
    """
    @brief Calculates the standard deviation of white reference dark values.

    This function takes a list of white reference dark value arrays, computes the standard deviation across them,
    and prints the resulting standard deviation array. It is used to assess the variability in dark measurements
    from multiple white reference scans.

    @param white_reference_dark_L List of arrays, each representing dark values from a white reference scan.
    @return None. Prints the calculated standard deviation array.
    """

    global white_reference_FN_L

    white_reference_FN_L = []

    white_reference_D = {}

    white_reference_value_L = []

    white_reference_value_standard_deviation_L = []

    white_reference_value_n_scans_L = []

    white_reference_dark_L = []

    white_reference_dark_standard_deviation_L = []

    white_reference_dark_n_scans_L = []

    # Process the white reference data
    for json_FPN in json_FPN_L:

        FN = path.split(json_FPN)[1]

        if FN.startswith('whiteref'):

            white_reference_FN_L.append(path.splitext(FN)[0])

            if process.verbose:

                print('Whiteref processing:', json_FPN)

            white_reference = Extract_white_reference_json_v089(project_FP, process, json_FPN) 

            if white_reference:

                white_reference_value_L.append( white_reference['value_A'].tolist())

                white_reference_value_standard_deviation_L .append( white_reference['value_standard_deviation_A'].tolist())

                white_reference_value_n_scans_L.append( white_reference['n_repeats'])

                white_reference_dark_L.append( white_reference['dark_A'].tolist())

                white_reference_dark_standard_deviation_L.append( white_reference['dark_standard_deviation_A'].tolist())

                white_reference_dark_n_scans_L.append( white_reference['n_dark_repeats'])

                white_reference_D[path.getctime(json_FPN)] = white_reference

    Calculate_white_reference_statistics(white_reference_value_L, 
                                            white_reference_value_standard_deviation_L, 
                                            white_reference_value_n_scans_L,
                                            white_reference_dark_L,
                                            white_reference_dark_standard_deviation_L,
                                            white_reference_dark_n_scans_L)
    
    return white_reference_D

def Process_xspectre_json_v089(project_FP, process):
    """
    @brief Imports and processes xspectre JSON data v089 file for AI4SH.

    This function reads data JSON files with both data and metadata, validates their contents,
    extracts relevant parameters, and calls a function to process each data record.

    @param process An object containing parameters and file paths for method and data CSV files.
    @return None. Prints error messages if files or parameters are invalid.
    """

    global position_date_F, position_date_str_L

    position_date_str_L = []

    from src.utils import Os_walk

    ''' The following commands are for creating the csv file for locations, settings, sample dates and coordinates
    
    position_date_FPN = "/Users/thomasgumbricht/projects/ai4sh_sueloanalys/coordinates/AI4SH_point_name_position_sampledate"
    
    position_date_FPN = '%s_%s.csv' % (position_date_FPN, process.parameters.pilot_site.lower())

    position_date_F = open(position_date_FPN, 'w')
    
    position_date_F.write('pilot_country,pilot_site,sampling_log,point_id,sample_date,min_depth,max_depth,position_name,setting,latitude,longitude\n')
    '''
    json_FPN_L = Os_walk(process.parameters.data_src_FP, '.json')

    white_reference_D = None
    
    #if 'spectra' in project_FP:
    if process.parameters.procedure == 'xspectre-spectra':

        white_reference_D = Get_all_white_reference_data(project_FP, process, json_FPN_L)

    if process.overwrite:

        print('Overwrite is set to True, all existing JSON files will be deleted before processing new data')

        for item in ['ai4sh','xspectre','ossl']:

            dst_FP= '%s_%s' %(process.parameters.dst_FP,item)

            dst_FP = Full_path_locate(project_FP, dst_FP, True)

            Remove_path(dst_FP)

    # Get the global file for locus, sample date and coordinates
 

    #point_name_position_sampledate_FPN = Full_path_locate(project_FP,process.parameters.point_name_position_sampledate_FPN)

    #if point_name_position_sampledate_FPN is None:

    #    print ('❌  ERROR - you must link to a csv file with locations, settings, sample dates and coordinates.')

    #    print('❌  File not found: %s' % (process.parameters.point_name_position_sampledate_FPN))

    #    return None

    coordinate_D = Coordinates_fix(project_FP,process.parameters.point_name_position_sampledate_FPN)

    if not coordinate_D:

        print ('❌  ERROR - reading the location, setting, sample date and coordinate csv file failed.')

        print('❌  File: %s' % (process.parameters.point_name_position_sampledate_FPN))

        return None

    for json_FPN in json_FPN_L:

        FN = path.split(json_FPN)[1]

        if FN.startswith('whiteref'):

            continue

        print('Processing:', json_FPN)
        
        Extract_xspectre_json_v089(project_FP, process, json_FPN, white_reference_D, coordinate_D)

    # position_date_F.close()