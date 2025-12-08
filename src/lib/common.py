'''
Created 29 September 2025

thomasg
'''

# Standard library imports
from os import path, makedirs

from copy import deepcopy

# Third party imports
#import numpy as np

# Package application imports
from src.utils import  Delta_days, Dump_json, Full_path_locate, Remove_path, Write_csv_header_data

# Default variables
COMPULSARY_DATA_RECORDS = ['pilot_country','pilot_site','point_id','min_depth','max_depth','sample_date',
                                'sample_preparation__name','subsample','replicate','sample_analysis_date','sample_preservation__name',
                                'sample_transport__name','transport_duration_h','sample_storage__name','user_analysis__email',
                                'user_sampling__email','user_logistic__email', 'procedure',
                                'instrument_brand__name','instrument_model__name','instrument_id',
                                'analysis_method__name']

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
                                'pilot_site': ['campaignshortid',2],
                                'sample_date': ['sampling','sampledate'],
                                'sample_preparation__name': ['sampling','prepcode'],
                                'sample_analysis_date': ['scandate'],
                                'sample_preservation__name': None,
                                'sample_transport__name': None,
                                'sample_storage__name': None,
                                'transport_duration_h': 0,
                                'replicate': 0,
                                'subsample': 'a'
                            }

XSPECTRE_FLAT_DATA_L = [
    "campaign",
    "log_name",
    "log_date",
    "log_user",
    "sample_name",
    "substance_class__name",
    "substance_order__name",
    "substance_family__name",
    "substance_species__name",
    "substance_brand__name",
    "substance_date",
    "subsample",
    "replicate",
    "sampling_timestamp",
    "position__name",
    "longitude",
    "latitude",
    "min_depth",
    "max_depth",
    "layer_name",
    "sample_preparation__name",
    "sample_preservation__name",
    "sample_storage__name",
    "analysis__name",
    "indicator__name",
    "unit",
    "value",
    "standard_deviation",
    "n_repeats",
    "procedure__name",
    "method__name",
    "instrument_brand__name",
    "instrument_model__name",
    "instrument_id",
    "muzzle_code",
    "muzzle_hue",
    "muzzle_id",
    "dark_repeat",
    "head_trail_repeat",
    "led_mv",
    "stabilisation_time_ms",
    "scan_tuning__code",
    "n_value_below_zero",
    "n_value_above_unity",
    "n_value_missing",
    "n_value_saturated",
    "n_reference_saturated"
]

REPLICATE_D = {0:0, '0':0, 1:1, 2:2, 3:3, 4:4, 5:5, 6:6, '1':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9, 
               'a': 0, 'b':1,'c':2,'d':3,'e':4,'f':5,'g':6,'h':7,'i':8,'j':9,'k':10,
                'ab':2,'ac':3,'x':9} 

SUBSAMPLE_D = {'None':None,'none':None,'a':"a",'b':"b",'c':"c",'d':"d",'e':"e",'f':"f",'g':"g",'h':"h",'i':"i",'j':"j",'k':"k",
               'a1':"a",'a2':"b",'a3':"c",'a4':"d",'b1':"e",'b2':"f",'b3':"g",
                  'c1':"h",'c2':"i",'c3':"j",'d1':"k",'d2':"l",'d3':"m",'1':"a",'2':"b",'3':"c",'4':"d",'5':"e",'6':"f",'7':"g",'8':"h",'9':"i"}

METHOD_D = {'npkphcth-s':"penetrometer",'ph-ise':"ise-ph",'ise-ph':"ise-ph",'bob':'sear','sear':'sear'}

PREPCODE_D = {'None':'soil-undisturbed-in-situ','none':'soil-undisturbed-in-situ',
              'field':'soil-undisturbed-in-situ','no':'soil-undisturbed-in-situ','n0':'soil-undisturbed-in-situ',
              'in-situ':'soil-undisturbed-in-situ','insitu':'soil-undisturbed-in-situ','undisturbed':'soil-undisturbed-in-situ',
              'mx':"mixed-untreated-soil-in-lab",'mx-lab':"mixed-untreated-soil-in-lab",'mixed':"mixed-untreated-soil-in-lab", 
              'h2o-iso':'mixed-untreated-soil-in-lab',
              'ds':'dried-sieved-soil-in-lab','cu':'robert-minarik-cu',
              'd10':'xspectre-d10','d20':'xspectre-d20',
              'post-infiltration':'soaked',
              'dry-pick-soak':'dried-aggregate-select+soaked',
              'eo-data':'eo-data'}

INVERSE_PREPCODE_D = {'soil-undisturbed-in-situ':'no-prep',
              'mixed-untreated-soil-in-lab':'mix-wet',
              'dried-sieved-soil-in-lab':'dried-sieved',
              'robert-minarik-cu':'rm-cu',
              'xspectre-d10':'d10',
              'xspectre-d20':'d20',
              'soaked':'post-infiltration',
              'dried-aggregate-select+soaked':'dry-pick-soak',
              'eo-data':'eo-data'}

INDICATOR_D = {'tds':'total-dissolved-solids','ec':'electrical-conductivity','electrical conductivity':'electrical-conductivity',
               'salinity':'salinity',
               'ph':'ph(water)','ph(water)':'ph(water)', 'toc':'total-organic-carbon','soc':'soil-organic-carbon',
               'tot-n':'total-nitrogen','total-nitrogen':'total-nitrogen',
               'cation exchange capacity':'cation-exchange-capacity','cec':'cation-exchange-capacity',
               'ca2+':'calcium','mg2+':'magnesium','na+':'sodium','k+':'potassium',
               'olsen phosphorus':'olsen-phosphorus','available phosphorus':'olsen-phosphorus','p-olsen':'olsen-phosphorus',
               'clay (<0,002 mm)':'clay(<0.002mm)',
               'clay (<0.002 mm)':'clay(<0.002mm)','fine silt (0.002-0.02 mm)':'fine-silt(0.002-0.02mm)',
               'coarse silt (0.02-0.06 mm)':'coarse-silt(0.02-0.06mm)',
               'fine sand (0.06-0.2 mm)':'fine-sand(0.06-0.2mm)','coarse sand (0.2-2.0 mm)':'coarse-sand(0.2-2.0mm)',
               'clay(<0,002mm)':'clay(<0.002mm)',
                'clay(<0.002mm)':'clay(<0.002mm)','fine-silt(0.002-0.02mm)':'fine-silt(0.002-0.02mm)',
                'coarse-silt(0.02-0.06mm)':'coarse-silt(0.02-0.06mm)',
                'fine-sand(0.06-0.2mm)':'fine-sand(0.06-0.2mm)','coarse-sand(0.2-2.0mm)':'coarse-sand(0.2-2.0mm)',
                'temp':'temperature','temperature':'temperature','sm':'soil-moisture-volumetric-content',
               'water content':'soil-moisture-volumetric-content', 'bulk density':'bulk-density','bd':'bulk-density',
               'potassium':'potassium','phosphorus':'phosphorus','nitrogen':'nitrogen',
               'leu':'LEU','gla':'GLA','glu':'GLS','pho':'PHO','xyl':'XYL',
               'prokaryotes_alpha_observed_richness':'Prokaryotes_alpha_observed_richness',
               'prokaryotes_alpha_chao1_estimated richness':'Prokaryotes_alpha_chao1_estimated richness',
               'prokaryotes_functional_prediction_nitrogen_fixation':'Prokaryotes-functional-prediction-nitrogen-fixation',
               'prokaryotes_alpha_pielou_e':'Prokaryotes-alpha-pielou-e',
               'prokaryotes_alpha_shannon':'Prokaryotes-alpha-shannon',
               'prokaryotes_alpha_simpson':'Prokaryotes-alpha-simpson',
               'prokaryotes_functional_prediction_chemoheterotrophy':'Prokaryotes-functional-prediction-chemoheterotrophy',
               'prokaryotes_functional_prediction_human_pathogens_all':'Prokaryotes-functional-prediction-human-pathogens-all',
                'fungi_alpha_observed_richness':'Fungi-alpha-observed-richness',
                'fungi_alpha_chao1_estimated richness':'Fungi-alpha-chao1-estimated-richness',
                'prokaryotes_alpha_dominance':'Prokaryotes-alpha-dominance',
                'fungi_alpha_dominance':'Fungi-alpha-dominance',
                'fungi_alpha_pielou_e':'Fungi-alpha-pielou-e',
                'fungi_alpha_shannon':'Fungi-alpha-shannon',
                'fungi_alpha_simpson':'Fungi-alpha-simpson',
                'fungi_funtional_prediction_ectomycorrhizal fungi':'Fungi-funtional-prediction-Ectomycorrhizal-fungi',
                'fungi_funtional_prediction_arbuscular mycorrhizal fungi':'Fungi-funtional-prediction-Arbuscular-mycorrhizal-fungi',
                'fungi_funtional_prediction_fungal saprotrophs':'Fungi-funtional-prediction-fungal-saprotrophs',
                'fungi_funtional_prediction_fungal plant pathogens':'Fungi-funtional-prediction-fungal-plant-pathogens',
                'prokaryotes-alpha-observed-richness':'Prokaryotes-alpha-observed-richness','prokaryotes-alpha-chao1-estimated-richness':'Prokaryotes-alpha-chao1-estimated-richness',
                'prokaryotes-functional-prediction-nitrogen-fixation':'Prokaryotes-functional-prediction-nitrogen-fixation',
                'prokaryotes-alpha-pielou-e':'Prokaryotes-alpha-pielou-e',
                'prokaryotes-alpha-shannon':'Prokaryotes-alpha-shannon',
                'prokaryotes-alpha-simpson':'Prokaryotes-alpha-simpson',
                'prokaryotes-functional-prediction-chemoheterotrophy':'Prokaryotes-functional-prediction-chemoheterotrophy',
                'prokaryotes-functional-prediction-human-pathogens-all':'Prokaryotes-functional-prediction-human-pathogens-all',
                'fungi-alpha-observed-richness':'Fungi-alpha-observed-richness',
                'fungi-alpha-chao1-estimated-richness':'Fungi-alpha-chao1-estimated-richness',
                'prokaryotes-alpha-dominance':'Prokaryotes-alpha-dominance',
                'fungi-alpha-dominance':'Fungi-alpha-dominance',
                'fungi-alpha-pielou-e':'Fungi-alpha-pielou-e',
                'fungi-alpha-shannon':'Fungi-alpha-shannon',
                'fungi-alpha-simpson':'Fungi-alpha-simpson',
                'fungi-funtional-prediction-ectomycorrhizal-fungi':'Fungi-funtional-prediction-Ectomycorrhizal-fungi',
                'fungi-funtional-prediction-arbuscular-mycorrhizal-fungi':'Fungi-funtional-prediction-Arbuscular-mycorrhizal-fungi',
                'fungi-funtional-prediction-fungal-saprotrophs':'Fungi-funtional-prediction-fungal-saprotrophs',
                'fungi-funtional-prediction-fungal-plant-pathogens':'Fungi-funtional-prediction-fungal-plant-pathogens',
                'microbial-c':'Microbial-C','fungi':'fungi-fraction','bacteria':'bacteria-fraction',
                'porosity':'porosity','final infiltration rate':'final-infiltration-rate','sorptivity':'sorptivity',
                'saturated hydraulic conductivity':'saturated-hydraulic-conductivity','field capacity':'field-capacity',
                'plant available water':'plant-available-water','saturated water content':'saturated-water-content',
                'hg water pressure head scale parameter':'hg-water-pressure-head-scale-parameter',
                'n shape parameter of retention curve':'n-shape-parameter-of-retention-curve',
                'm shape parameter of retention curve':'m-shape-parameter-of-retention-curve',
                'aggregate stability index':'aggregate-stability-index',
                'spectra':'reflectance','reflectance':'reflectance',
                'dfme_edtm':'dfme-edtm',
                'geomorphon_edtm':'geomorphon-edtm',
                'hillshade_edtm':'hillshade-edtm',
                'ls.factor_edtm':'ls.factor-edtm',
                'maxic_edtm':'maxic-edtm',
                'minic_edtm':'minic-edtm',
                'neg.openness_edtm':'neg.openness-edtm',
                'pos.openness_edtm':'pos.openness-edtm',
                'pro.curv_edtm':'pro.curv-edtm',
                'shpindx_edtm':'shpindx-edtm',
                'slope.in.degree_edtm':'slope.in.degree-edtm',
                'twi_edtm':'twi-edtm',
                'crop.type_eucropmap.v1':'crop.type-eucropmap.v1'
}             

# TG TO DO - COMPLETE UNITS
UNIT_D = {} 
UNIT_D['C'] = {'property_abbreviation':'temp', 'property_full':'termperature', 'unit_abbreviation':'C', 'unit_full':'degree Celsius', 'add':0, 'multiply':1 }

class common_json_db:
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
        self.process = process
        self.project_FP = project_FP
        self.process_parameters = process.parameters
        self.process_parameters_D = dict(list(process.parameters.__dict__.items()))
        self.coordinate_D = coordinate_D

    def _Set_dst_FP(self):
        """
        @brief Sets the destination folder path and creates it if it does not exist.

        This function assigns the destination folder path from the process parameters to the instance variable `dst_FP`.
        If the folder does not exist, it is created.

        @details
        - Retrieves the destination folder path from `self.process_parameters.dst_FP`.
        - Checks if the folder exists using `os.path.exists`.
        - Creates the folder if it does not exist.

        @return None
        """
        self.dst_FP_D = {}

        for item in ['ai4sh','xspectre']:

            dst_FP, method_FP = path.split(self.process_parameters.dst_FP)

            #dst_FP= '%s_%s' %(self.process_parameters.dst_FP,item)

            dst_FP = path.join(dst_FP, item, method_FP)

            dst_FP = Full_path_locate(self.project_FP, dst_FP, True)

            self.dst_FP_D[item] = dst_FP

            if not path.exists(dst_FP):

                makedirs(dst_FP)

    def _Locus(self):
        """
        @brief Generates a unique locus identifier based on pilot country, pilot site, point ID, and canopy setting.

        This function constructs a locus string by concatenating the pilot country, pilot site, point ID, and canopy setting
        from the record dictionary. The resulting locus is stored in the instance variable `locus`.

        @details
        - The locus is formatted as "pilot_country_pilot_site_pointID_canopy".
        - All components are converted to lowercase to ensure consistency.

        @return None
        """
        #position_name = '%s_%s' %(self.site['name'].lower(), self.point['name'].lower())

        # NOTE TGTOD=: check against coordinate_D as master
        locus =  {"position__name":self.record_D['position_name'],
                "point": self.record_D['point_id'],
                "setting": self.record_D['setting'],
                "latitude": self.point['latitude'],
                "longitude": self.point['longitude'],
                "min_depth": self.sample["min_depth"], 
                "max_depth": self.sample['max_depth']}
            
        return locus
       
    def _Assemble_sample_event_AI4SH_AI4SH(self):
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

            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('value_standard_deviation')
            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('dark_value')
            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('dark_value_standard_deviation')

        except:

            pass

        self.equipment_method_copy_D = deepcopy(self.equipment_method_D)

        try:

            for item in self.equipment_method_copy_D[self.equipment]: 
            
                success = self.equipment_method_copy_D[self.equipment][item][0].pop('procedure')

                #success = self.equipment_method_copy_D[self.equipment][item][0].pop('procedure')

            #analysis_method = [self.equipment_method_copy_D]
            analysis_method = {self.equipment: [self.equipment_method_copy_D[self.equipment]]}
            analysis_method = {self.equipment: [self.equipment_method_copy_D[self.equipment]]}
            #analysis_method = [self.record_D['procedure']]

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
        
    def _Assemble_sample_event_AI4SH_xspectre(self):
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

            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('value_standard_deviation')
            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('dark_value')
            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('dark_value_standard_deviation')

        except:

            pass

        self.equipment_method_copy_D = deepcopy(self.equipment_method_D)

        try:

            for item in self.equipment_method_copy_D[self.equipment]: 
            
                success = item.pop('procedure')

        except:

            pass

        
        try:

            analysis_method = {self.record_D['procedure']: self.equipment_method_copy_D[self.equipment]}

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
         
    def _Assemble_sample_event_xspectre_xspectre(self):
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

            self.xspectre_method_D[self.equipment][0]['spectra_scan_tuning'].pop('value_standard_deviation')
            self.xspectre_method_D[self.equipment][0]['spectra_scan_tuning'].pop('dark_value')
            self.xspectre_method_D[self.equipment][0]['spectra_scan_tuning'].pop('dark_value_standard_deviation')

        except:

            pass

        locus = self._Locus()
        
        analysis = {} 

        # Re-organise the analysis method dictionary to have indicators as keys
        for item in self.equipment_method_D[self.equipment]: 
            
            indicator = item['indicator__name']

            analysis[indicator] = item  

        if hasattr(self, 'xspectre_spectra_meta_D') and self.process.parameters.extended_metadata:

            self.observation_metadata['metadata'] = self.xspectre_spectra_meta_D

            self.observation_metadata['scan_dn'] = self.original_scan_dn_D

            self.observation_metadata['white_reference'] = self.white_reference_D

            #self.observation_metadata['muzzle'] = self.xspectre_spectra_meta_D['muzzle']

            #self.observation_metadata['spectra_scan_tuning'] = self.xspectre_spectra_meta_D['spectra_scan_tuning']

            #self.observation_metadata['error'] = self.xspectre_spectra_meta_D['error']

        observation = {**self.observation_metadata, "analysis": analysis}
        sample = {"sample": self.sample['name']}
        sampling_log = {"sampling_log": self.sampling_log}
        locus = {"locus": locus}
        sample_event = {"campaign":self.data_source['name'], **sampling_log, **sample, **locus, "observation": observation}
        return sample_event  

    def _Assemble_sample_event_to_xspectre_from_ai4sh(self):
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

            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('value_standard_deviation')
            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('dark_value')
            self.equipment_method_D[self.equipment][0]['spectra_scan_tuning'].pop('dark_value_standard_deviation')

        except:

            pass
        
        analysis = {} 

        # Re-organise the analysis method dictionary to have indicators as keys
        for procedure in self.equipment_method_D[self.equipment]: 

            for item in self.equipment_method_D[self.equipment][procedure]:
            
                indicator = item['indicator__name']
  
                analysis[indicator] = item
        locus = self._Locus()
        '''
        locus = '%s_%s' %(self.site['name'].lower(), self.point['name'].lower())
        if self.coordinate_D:
            locus =  {"position__name":locus,
                      "point": self.record_D['point_id'],
                      "setting": self.record_D['canopy'],
                    "latitude": self.point['latitude'],
                        "longitude": self.point['longitude'],
                    "min_depth": self.sample["min_depth"], 
                    "max_depth": self.sample['max_depth']}
        else:
            locus =  {"position__name":locus,
                      "point": self.record_D['point_id'],
                      "setting": self.record_D['canopy'],
                    "min_depth": self.sample["min_depth"], 
                    "max_depth": self.sample['max_depth']}
        '''
        observation = {**self.observation_metadata, "analysis": analysis}
        sample = {"sample": self.sample['name']}
        sampling_log = {"sampling_log": self.sampling_log}
        locus = {"locus": locus}
        sample_event = {"campaign":self.data_source['name'], **sampling_log, **sample, **locus, "observation": observation}
        return sample_event  
    
    def _Set_equipment_method(self):
        """
        @brief Initializes the equipment-method mapping dictionary.

        @details
        - Creates a dictionary (equipment_method_D) where each unique equipment is a key, and its value is a dictionary of methods associated with that equipment.
        - For each method in method_D, associates it with the corresponding equipment in equipment_D and initializes an empty list for storing observations.

        @return None
        """

        self.equipment_method_D = {self.equipment: {}}
        
        self.equipment_method_D[self.equipment] = []

    def _Get_observation_measurements_xspectre(self):
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

        for indicator_key in self.indicator_L:
            '''
            '''

            indicator__name = '%s_%s' %( self.process.parameters.procedure, INDICATOR_D[indicator_key])
   
            if self.process.parameters.instrument_model__name == 'ise-ph-soil':

                indicator__name = indicator__name.replace('ph(water)','ph(soil)')

            if indicator__name == 'xspectre-penetrometer_ph(water)':

                indicator__name = 'xspectre-penetrometer_ph(soil)'

            #if 'standard_deviation' in self.record_D and self.record_D['standard_deviation'][indicator_key]:
            if 'standard_deviation' in self.record_D:
 
                observation_D =  {'value': self.record_D['value'][indicator_key], 
                            'standard_deviation': self.record_D['standard_deviation'][indicator_key],
                            'unit__name': self.record_D['unit__name'][indicator_key],
                            'indicator__name':  indicator__name,
                            'procedure': self.record_D['procedure'],
                            'analysis_method__name': self.record_D['analysis_method__name'], #METHOD_D[self.equipment],
                            'instrument_brand__name': self.record_D['instrument_brand__name'],
                            'instrument_model__name': self.record_D['instrument_model__name'],
                            'instrument_id': self.record_D['instrument_id']}

            else:

                observation_D =  {'value': self.record_D['value'][indicator_key],
                            'unit__name': self.record_D['unit__name'][indicator_key],
                            'indicator__name':  indicator__name,
                            'procedure': self.record_D['procedure'],
                            'analysis_method__name': self.record_D['analysis_method__name'], #METHOD_D[self.equipment],
                            'instrument_brand__name': self.record_D['instrument_brand__name'],
                            'instrument_model__name': self.record_D['instrument_model__name'],
                            'instrument_id': self.record_D['instrument_id']}

            self.equipment_method_D[self.equipment].append(observation_D)

            self.xspectre_method_D = {self.equipment: []}

            self.xspectre_method_D[self.equipment].append(observation_D)

    def _Get_observation_measurements_AI4SH(self):
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

        for indicator_key in self.indicator_L:
            '''
            '''

            indicator__name = '%s_%s' %( self.process.parameters.procedure, INDICATOR_D[indicator_key])
            

            if 'standard_deviation' in self.record_D and self.record_D['standard_deviation'][indicator_key]:
 
                observation_D =  {'value': self.record_D['value'][indicator_key], 
                            'standard_deviation': self.record_D['standard_deviation'][indicator_key],
                            'unit__name': self.record_D['unit__name'][indicator_key],
                            'indicator__name':  indicator__name,
                            'procedure': self.record_D['procedure'][indicator_key],
                            'analysis_method__name': self.record_D['analysis_method__name'][indicator_key], #METHOD_D[self.equipment],
                            'instrument_brand__name': self.record_D['instrument_brand__name'][indicator_key],
                            'instrument_model__name': self.record_D['instrument_model__name'][indicator_key],
                            'instrument_id': self.record_D['instrument_id'][indicator_key]}

            else:

                observation_D =  {'value': self.record_D['value'][indicator_key],
                            'unit__name': self.record_D['unit__name'][indicator_key],
                            'indicator__name':  indicator__name,
                            'procedure': self.record_D['procedure'][indicator_key],
                            'analysis_method__name': self.record_D['analysis_method__name'][indicator_key], #METHOD_D[self.equipment],
                            'instrument_brand__name': self.record_D['instrument_brand__name'][indicator_key],
                            'instrument_model__name': self.record_D['instrument_model__name'][indicator_key],
                            'instrument_id': self.record_D['instrument_id'][indicator_key]}

            self.equipment_method_D[self.equipment][self.method_D[self.indicator_method_D[indicator_key]]].append(observation_D)
            
    
    def _Set_locus_sample_date_coordinate(self):
        """
        @brief Sets the locus, sample date, and coordinate information for the record.

        This function generates a unique locus identifier based on pilot country, pilot site, point ID, and canopy setting.
        It also sets the sample date and coordinate information in the record dictionary.

        @details
        - Calls the `_Locus` method to generate the locus identifier.
        - Sets the `locus`, `sample_date`, and `coordinate_D` attributes in the record dictionary.

        @return None
        """
  
        #self.record_D['sample_date'] = self.record_D['sample_date']
        # Get the locus from the original data, and then retrieve this locus data from coordinate_D
        locus = '%s-%s_%s' %(self.record_D['pilot_country'].lower(), self.record_D['pilot_site'].lower(), self.record_D['point_id'].lower())

        if not locus in self.coordinate_D:

            print ('❌  ERROR - locus not found in coordinate_D: '+locus)
            
            print (' To fix this problem make sure to add the locus to the file:\n %s' %(self.process_parameters.point_name_position_sampledate_FPN))

            return None
        
        for item in self.coordinate_D[locus]:

            self.record_D[item] = self.coordinate_D[locus][item].lower() if isinstance(self.coordinate_D[locus][item], str) else self.coordinate_D[locus][item]

        self.record_D['locus'] = locus

        self.record_D['site_id'] =  '%s-%s' %(self.record_D['pilot_country'], self.record_D['pilot_site'])

        return True
    
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

    def _Check_set_final_record(self, FPN):

        # Check if all compulsory data records are set
        for item in COMPULSARY_DATA_RECORDS:

            if item not in self.record_D:

                if item in self.process_parameters_D:

                    if isinstance(self.process_parameters_D[item], str):
                        self.record_D[item] = self.process_parameters_D[item].lower()
                    else:
                        self.record_D[item] = self.process_parameters_D[item]

                else:

                    print ('❌  ERROR - compulsory data not found: '+item)
                    print (' You can add <'+item+'> parameter to the process file:')
                    print('  - the process file: %s' %(FPN))
                    
                    return None
                
        # Check and convert subsample, replicate and sample preparation codes
        if not self.record_D['subsample'] in SUBSAMPLE_D:

            print ('❌  ERROR - subsample id not recognised: <%s>' % self.record_D['subsample'])

            return None
        
        self.record_D['subsample'] = SUBSAMPLE_D[self.record_D['subsample']]

        if not self.record_D['replicate'] in REPLICATE_D:
            print ('❌  ERROR - replicate id not recognised: <%s>' % self.record_D['replicate'])

            return None
        
        self.record_D['replicate'] = REPLICATE_D[self.record_D['replicate']]

        if not self.record_D['sample_preparation__name'] in PREPCODE_D:
            print ('❌  ERROR - sample preparation name not recognised: %s' % self.record_D['sample_preparation__name'])

            return None
        
        self.record_D['sample_preparation__name'] = PREPCODE_D[self.record_D['sample_preparation__name']]

        # Calculate storage duration in hours

        if isinstance(self.record_D['procedure'], dict) and self.record_D['procedure'][self.indicator_L[0]] == 'eo-data':

            self.record_D['storage_duration_h'] = 0

            self.record_D['sample_date'] = 0

        else:
            self.record_D['storage_duration_h'] = max(0, 24*(Delta_days(self.record_D['sample_date'], self.record_D['sample_analysis_date'])))

            # If duration is negative, set analysis date to sampling data
            if self.record_D['storage_duration_h'] == 0:
                self.record_D['sample_analysis_date'] = self.record_D['sample_date']

        # convert all text records to lower case
        for item in self.record_D:

            if isinstance(self.record_D[item], str):

                self.record_D[item] = self.record_D[item].lower()

        return True
    
    def _Get_spectra_measurements_OLD(self, xspectre_json_D):
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

        #for indicator_key in xspectre_json_D['sensing']:

        indicator__name = 'xspeectre_%s_%s' %( self.process.parameters.instrument_model__name, INDICATOR_D[self.record_D['indicator__name']])
                
        if 'samplestd' in xspectre_json_D:

            observation_D =  {'value': self.record_D['reflectance_value_A'].tolist(), 
                        'standard_deviation': self.record_D['reflectance_standard_deviation_A'].tolist(), 
                        'unit__name': self.record_D['unit__name'],
                        'indicator__name':  indicator__name,
                        'analysis_method__name': self.record_D['analysis_method__name'], #METHOD_D[self.equipment],
                        'instrument_brand__name': self.record_D['instrument_brand__name'],
                        'instrument_model__name': self.record_D['instrument-_model__name'],
                        'instrument_id': self.record_D['instrument_id']}
        else:

            observation_D =  {'value': xspectre_json_D['samplemean'], 
                        'unit__name': self.record_D['unit__name'],
                        'indicator__name':  indicator__name,
                        'analysis_method__name': self.record_D['analysis_method__name'], #METHOD_D[self.equipment],
                        'instrument_brand__name': self.record_D['instrument_brand__name'],
                        'instrument_model__name': self.record_D['instrument-_model__name'],
                        'instrument_id': self.record_D['instrument_id']}

        spectra_scan_D =  {
                        'dark_repeats': xspectre_json_D[self.equipment]['darkrepeats'],
                        'head_trail_repeats': xspectre_json_D[self.equipment]['headtrail'],
                        'led_mv': xspectre_json_D[self.equipment]['parameters']['LED_mset_mV'],
                        'stabilisation_time_ms': xspectre_json_D[self.equipment]['parameters']['stabilistaiontime'],
                        'value_standard_deviation': xspectre_json_D['samplestd'],
                        'dark_value': xspectre_json_D['darkmean'],
                        'dark_value_standard_deviation': xspectre_json_D['darkstd']
                        }
            
        scan_tuning__code = str(xspectre_json_D[self.equipment]['parameters']['scantuning']['nIntegrationTimes'])

        for i in range(xspectre_json_D[self.equipment]['parameters']['scantuning']['nIntegrationTimes']):

            scan_tuning__code += '_%s-%s-%s' % (xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes'][str(i)][0],
                                            xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes'][str(i)][1],
                                            xspectre_json_D[self.equipment]['parameters']['scantuning']['integrationtimes'][str(i)][2])
   
            spectra_scan_D['scan_tuning__code'] = scan_tuning__code

            observation_D['spectra_scan_tuning'] = spectra_scan_D 

        self.equipment_method_D[self.equipment].append(observation_D)

    def _Set_point(self):
        """
        @brief Sets unique identifiers for pilot, site, point, and sample, and initializes related metadata dictionaries.

        @return None
        """

        locus = '%s-%s_%s' %(self.record_D['pilot_country'].lower(), self.record_D['pilot_site'].lower(), self.record_D['point_id'].lower())
 
        self.point = {
        "name": self.record_D['point_id'].lower(),
        "latitude": self.coordinate_D[locus]['latitude'],
        "longitude": self.coordinate_D[locus]['longitude'],
        "setting": self.coordinate_D[locus]['setting'].lower()}
        
    def _Set_site(self):
        """
        @brief Sets unique identifiers for pilot, site, point, and sample, and initializes related metadata dictionaries.

        @return None
        """
        
        self.site = {
            "name": self.record_D['site_id'].lower()
        }

    def _Set_data_source(self):
        """
        @brief Sets unique identifiers for pilot, site, point, and sample, and initializes related metadata dictionaries.

        @return None
        """

        self.data_source = {
            "name": '%s_%s' % ('ai4sh',self.record_D['site_id'].lower())
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

        self.observation_metadata = {
                        "sample_preparation__name": self.record_D['sample_preparation__name'],
                        "person__email": self.record_D['user_analysis__email'],
                        "subsample": self.record_D['subsample'],
                        "replicate": self.record_D['replicate'],
                        "n_repeats": self.record_D['n_repetitions'],
                        "date_stamp": self.record_D['sample_analysis_date'],
                        "logistic": {
                          "sample_preservation__name": self.record_D['sample_preservation__name'],
                          "sample_transport__name": self.record_D['sample_transport__name'],
                          "transport_duration_h": self.record_D['transport_duration_h'],
                          "sample_storage__name": self.record_D['sample_storage__name'],
                          "storage_duration_h":  self.record_D['storage_duration_h'],
                          "person__email": self.record_D['user_logistic__email']}
                        }
        
        return True
    
    def _Set_sample(self):
        """
        """

        self.sample = {"name": self.record_D['sample_id'].lower(),
                    "min_depth": int(self.record_D['min_depth']),
                    "max_depth": int(self.record_D['max_depth'])}

    def _Set_sampling_log(self):
        """

        """

        self.sampling_log = {"name": self.record_D['sampling_log_id'].lower(),
                            "date_stamp": self.record_D['sample_date'],
                            "person__email": self.record_D['user_sampling__email']}
        
    def _Set_sample_id(self):
    
        self.record_D['sample_id'] =  self.record_D['sampling_log_id'].lower()+\
                    '_'+self.record_D['point_id'].lower()+\
                    '_'+self.record_D['min_depth']+\
                    '-'+self.record_D['max_depth']

    def _Dump_sample_json(self, sample_event, item):
        """
        @brief Dumps a sample event dictionary to a JSON file.

        This function creates a destination file name and path based on the sample ID and destination folder path.
        It then writes the provided sample event dictionary to the JSON file using the Dump_json function with indentation.
        After successful writing, it prints the path of the created JSON file.

        @param sample_event Dictionary containing the sample event data to be exported to JSON.

        @return None
        """

        instrument_model__name = self.record_D['instrument_model__name']
        instrument_model__id = self.record_D['instrument_id']

        if isinstance(self.record_D['instrument_model__name'], dict):
            
            if len(self.record_D['instrument_model__name']) == 1:

                instrument_model__name = list(self.record_D['instrument_model__name'].values())[0]

                instrument_model__id = list(self.record_D['instrument_id'].values())[0]
                
            else:

                instrument_model_set = set(self.record_D['instrument_model__name'].values())

                instrument_model_id_set = set(self.record_D['instrument_id'].values())

                if len(instrument_model_set) == 1:

                    instrument_model__name = instrument_model_set.pop()
                    instrument_model__id = instrument_model_id_set.pop()

                    if instrument_model__name == '0':

                        instrument_model__name = self.process.parameters.procedure
                else:

                    instrument_model__name = instrument_model__id = 'multiple'
                    
        dst_FN = '%s_%s_%s_%s_%s_%s_%s_%s.json' %(self.record_D['sample_id'], 
                                         self.record_D['subsample'], 
                                         self.record_D['replicate'], 
                                         self.record_D['setting'],
                                         INVERSE_PREPCODE_D[self.record_D['sample_preparation__name']], 
                                         instrument_model__name.replace('_','-'),
                                         instrument_model__id.replace('_','-'),
                                         self.record_D['sample_analysis_date'])
        
        if item == 'xspectre' and 'muzzle_code' in self.record_D:

            dst_FN = '%s_%s_%s_%s_%s_%s_%s_%s_%s_%s.json' %(self.record_D['sample_id'],
                                                 self.record_D['subsample'],
                                                 self.record_D['replicate'],
                                                 self.record_D['setting'],
                                                 INVERSE_PREPCODE_D[self.record_D['sample_preparation__name']],
                                                 instrument_model__name.replace('_','-'),
                                                 instrument_model__id.replace('_','-'),
                                                 self.record_D['muzzle_code'],
                                                 self.record_D['muzzle_formfactor'],
                                                 self.record_D['sample_analysis_date'])


        dst_FPN = path.join(self.dst_FP_D[item], dst_FN)

        # Write the updated json file
        success = Dump_json(dst_FPN, sample_event, indent=2)
        
        if not success:
        
            print('❌ %s Json post creation failed: %s' %(item,dst_FPN))
        
        elif self.process.verbose > 1:

            print('✅ %s Json post created successfully: %s' %(item,dst_FPN))

    def _Write_OSSL_csv(self, prefix, column_L, data_L_L):
        """
        @brief Writes data to a CSV file with specified columns.    


        This function takes a file path, a list of column headers, and a list of data rows, and writes them to a CSV file.          
        @param FPN (str): The file path where the CSV file will be saved.
        @param column_L (list): A list of column headers for the CSV file.
        @param data_L_L (list of lists): A list of data rows, where each row is a list of values corresponding to the columns.
        @return None
        """

        instrument_model__name = self.record_D['instrument_model__name']

        if self.process.parameters.procedure == 'wetlab':
            
            instrument_model__name = 'wetlab'
                    
        dst_FN = '%s_%s_%s_%s_%s.csv' %(prefix,self.record_D['site_id'], instrument_model__name,
                                         INVERSE_PREPCODE_D[self.record_D['sample_preparation__name']], 
                                         self.record_D['sample_analysis_date'])

        dst_FPN = path.join(self.dst_FP_D['ossl'], dst_FN)

        Write_csv_header_data(dst_FPN, column_L, data_L_L)