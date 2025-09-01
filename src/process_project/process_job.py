'''
Created on 5 jan 2025
Updated 11 Mar 2025

@author: thomasgumbricht

'''

# Standard library imports

from os import path, makedirs

# Third party imports

from base64 import b64encode
import re

# Package application imports

from src.utils import Read_json, Pprint_parameter, SetDiskPath #, Log

#from postgres import Db_connect, PG_session, PG_user_status, PG_check_user_settings

#from process_project.timestep import Timesteps

# from process_project.location import Location

# from process_project.compositon import Composition

# from process_project.layer import Layer

def Update_dict(user_D, default_D):
    '''
    '''

    d = {key: default_D.get(key, user_D[key]) for key in user_D}

    for key in default_D:

        if key not in d:

            user_D[key] = default_D[key]

def Check_param_instance(p, typeD, process_D, json_file_FN, p_str):

    def _Print_error_msg(error_msg):

        error_msg += '              (file: %s;  process nr %s)' %(json_file_FN, p_str)

        print (error_msg)

    if 'array' in typeD[p].lower() or typeD[p].lower()[0:3] == 'csv':
        
        # TGTODO The array (csvList) is a tricky alternative to solve here
        #if not isinstance(process_D[p], list):
        if not isinstance(process_D[p], str) and not isinstance(process_D[p], list):

            error_msg = '          ❌ WARNING parameter %s is not a list (%s)\n' %(p,process_D[p])

            _Print_error_msg(error_msg)

            return None

    elif typeD[p].lower()[0:3] == 'tex':

        if not isinstance(process_D[p], str):

            error_msg = '          ❌ WARNING parameter %s is not a string (%s)\n' %(p,process_D[p])

            _Print_error_msg(error_msg)

            return None

        if '_dir' in p.lower():

            if process_D[p][0] in ['/'] or process_D[p][len(process_D[p])-1] in ['/']:

                error_msg = '          ❌ WARNING parameter %s can not start or end with a slash (%s)\n' %(p,process_D[p])

                _Print_error_msg(error_msg)

                return None

    elif typeD[p].lower()[0:3] == 'int':

        if not isinstance(process_D[p], int) and not process_D[p].isdigit():

            error_msg = '          ❌ WARNING parameter %s is not an integer (%s)\n' %(p,process_D[p])

            _Print_error_msg(error_msg)

            return None

    elif typeD[p].lower()[0:3] == 'flo':

        if not isinstance(process_D[p], float):

            if not isinstance(process_D[p], int):

                error_msg = '          ❌ WARNING parameter %s is neither a float nor an integer (%s)\n' %(p,process_D[p])

                _Print_error_msg(error_msg)

                return None

    elif typeD[p].lower()[0:3] == 'boo':

        if not isinstance(process_D[p], bool):

            if isinstance(process_D[p], str) and process_D[p].lower() in ['true', '1']:

                process_D[p] = process_D[p].lower() == 'true'

            elif isinstance(process_D[p], str) and process_D[p].lower() in ['false','0']:

                process_D[p] = process_D[p].lower() == 'false'

            else:

                error_msg = '          ❌ WARNING parameter %s is not a boolean (%s)\n' %(p,process_D[p])

                _Print_error_msg(error_msg)

                return None

    return 1

class Struct(object):
    ''' Recursive class for building project objects
    '''
    def __init__(self, data):

        for name, value in data.items():

            setattr(self, name, self._wrap(value))

    def _wrap(self, value):

        if isinstance(value, (tuple, list, set, frozenset)):

            return type(value)([self._wrap(v) for v in value])

        else:

            return Struct(value) if isinstance(value, dict) else value

class Params():
    '''
    '''
    def __init__(self, default_parameter_D):
        '''
        '''

        #  REMOVE THE "process" list and convert first list item to a dict
        self.default_parameter_D = self._splitOutProcess(default_parameter_D, 0)

        if self.default_parameter_D['process']['verbose'] > 2:

            print ('    default_parameter_D:')

            Pprint_parameter(self.default_parameter_D)

    def _splitOutProcess(self, raw_D, process_nr):
        '''
        '''
        process_D = {}

        for key, value in raw_D.items():

            if key == 'process':

                process_D['process'] = raw_D['process'][process_nr]

            else:

                process_D[key] = value

        return process_D

    def _Set_user_params(self,user_parameter_D,json_file_FN):
        '''
        '''

        self.json_file_FN = json_file_FN

        # update user_parameter_D by filling in missing variables from the default parameter values
        Update_dict(user_parameter_D, self.default_parameter_D)

        self.user_parameter_D = user_parameter_D

    def _Set_layers(self):
        '''
        '''
        self.dstLayerD = {}
        self.srcLayerD = {}
        self._SetDstLayers()
        self._SetSrcLayers()

    def _Set_src_layers(self):
        '''
        '''

        self.srcCompL = []
        self.srcLayerExistD = {}; self.srcLayerNonExistD = {};
        self.srcLayerDateExistD = {}; self.srcLayerDateNonExistD = {}
        self.srcLayerCreateD = {}; self.srcLayerDateCreateD = {};

        for comp in self.srcCompD:

            self.srcCompL.append(comp)

        for locus in self.srcLocations.locusL:

            self.srcLayerExistD[locus] = {}; self.srcLayerNonExistD[locus] = {};
            self.srcLayerDateExistD[locus] = {}; self.srcLayerDateNonExistD[locus] = {}
            self.srcLayerCreateD[locus] = {}; self.srcLayerDateCreateD[locus] = {};

            for comp in self.srcCompD:

                self.srcLayerExistD[locus][comp] = []; self.srcLayerNonExistD[locus][comp] = [];
                self.srcLayerDateExistD[locus][comp] = []; self.srcLayerDateNonExistD[locus][comp] = []
                self.srcLayerCreateD[locus][comp] = []; self.srcLayerDateCreateD[locus][comp] = [];

        for locus in self.srcLocations.locusL:

            self.srcLayerD[locus] = {}

            for datum in self.srcPeriod.datumL:

                self.srcLayerD[locus][datum] = {}

                for comp in self.srcCompD:

                    if self.srcCompD[comp].ext.lower() in ['.txt','.csv','.none']:

                        self.srcLayerD[locus][datum][comp] = TextLayer(self.srcCompD[comp], self.srcLocations.locusD[locus], self.srcPeriod.datumD[datum])

                    elif self.srcCompD[comp].ext.lower() in [ '.shp']:

                        self.srcLayerD[locus][datum][comp] = VectorLayer(self.srcCompD[comp], self.srcLocations.locusD[locus], self.srcPeriod.datumD[datum])

                    else:

                        self.srcLayerD[locus][datum][comp] = RasterLayer(self.srcCompD[comp], self.srcLocations.locusD[locus], self.srcPeriod.datumD[datum])

                    if path.exists(self.srcLayerD[locus][datum][comp].FPN):

                        self.srcLayerExistD[locus][comp].append(self.srcLayerD[locus][datum][comp].FPN)
                        self.srcLayerDateExistD[locus][comp].append( datum )
                        #self.session._InsertLayer(self.pp.dstLayerD[self.locus][datum][comp],self.process.overwrite,self.process.delete)

                    else:

                        self.srcLayerNonExistD[locus][comp].append(self.srcLayerD[locus][datum][comp].FPN)
                        self.srcLayerDateNonExistD[locus][comp].append( datum )

                        warnstr = '❌ WARNING, source file does not exist: %(p)s' %{'p':self.srcLayerD[locus][datum][comp].FPN}

                        print (warnstr)

    def _Set_dst_layers(self):
        '''
        '''

        self.dstCompL = []
        self.dstLayerExistD = {}; self.dstLayerNonExistD = {};
        self.dstLayerDateExistD = {}; self.dstLayerDateNonExistD = {}
        self.dstLayerCreateD = {}; self.dstLayerDateCreateD = {};

        for comp in self.dstCompD:

            self.dstCompL.append(comp)

        for locus in self.dstLocations.locusL:

            self.dstLayerExistD[locus] = {}; self.dstLayerNonExistD[locus] = {};
            self.dstLayerDateExistD[locus] = {}; self.dstLayerDateNonExistD[locus] = {}
            self.dstLayerCreateD[locus] = {}; self.dstLayerDateCreateD[locus] = {};

            for comp in self.dstCompD:

                self.dstLayerExistD[locus][comp] = []; self.dstLayerNonExistD[locus][comp] = [];
                self.dstLayerDateExistD[locus][comp] = []; self.dstLayerDateNonExistD[locus][comp] = []
                self.dstLayerCreateD[locus][comp] = []; self.dstLayerDateCreateD[locus][comp] = [];

        for locus in self.dstLocations.locusL:

            self.dstLayerD[locus] = {}

            for datum in self.dstPeriod.datumL:

                self.dstLayerD[locus][datum] = {}

                for comp in self.dstCompD:

                    if self.dstCompD[comp].ext == '.shp':

                        self.dstLayerD[locus][datum][comp] = VectorLayer(self.dstCompD[comp], self.dstLocations.locusD[locus], self.dstPeriod.datumD[datum])

                    else:

                        self.dstLayerD[locus][datum][comp] = RasterLayer(self.dstCompD[comp], self.dstLocations.locusD[locus], self.dstPeriod.datumD[datum])
                    
                    if path.exists(self.dstLayerD[locus][datum][comp].FPN):

                        self.dstLayerExistD[locus][comp].append(self.dstLayerD[locus][datum][comp].FPN)
                        self.dstLayerDateExistD[locus][comp].append( datum )

                        if self.process.overwrite:

                            self.dstLayerCreateD[locus][comp].append(self.dstLayerD[locus][datum][comp].FPN)
                            self.dstLayerDateCreateD[locus][comp].append( datum )

                        else:
                            pass
                            #self.session._InsertLayer(self.pp.dstLayerD[self.locus][datum][comp],self.process.overwrite,self.process.delete)


                    else:

                        self.dstLayerNonExistD[locus][comp].append(self.dstLayerD[locus][datum][comp].FPN)
                        self.dstLayerDateNonExistD[locus][comp].append( datum )

                        self.dstLayerCreateD[locus][comp].append(self.dstLayerD[locus][datum][comp].FPN)
                        self.dstLayerDateCreateD[locus][comp].append( datum )

                    '''
                    #self.srcLayerD[locus][datum][comp] = self.srcCompD[comp]
                    #self.dstLayerD[locus][datum][comp]['comp'] = self.dstCompD[comp]
                    if (self.dstLayerD[locus][datum][comp]['comp'].celltype == 'vector'):
                        self.dstLayerD[locus][datum][comp]['layer'] = VectorLayer(self.dstCompD[comp], self.dstLocations.locusD[locus], self.srcPeriod.datumD[datum], self.dstpath)
                    else:
                        self.dstLayerD[locus][datum][comp]['layer'] = RasterLayer(self.dstCompD[comp], self.dstLocations.locusD[locus], self.srcPeriod.datumD[datum], self.dstpath)
                    '''


    def _Location(self, session):
        ''' Set the location for the source and destination of this process
        '''

        if not hasattr(self.process_S.user_project,'schemata') or self.process_S.user_project.schemata is None:

            self.process_S.process.schemata = None

            return True
        
        self.process_S.process.locus_L = []

        self.process_S.process.locus_D = {}

        # SELECT the schemata region type and coordinate system for source and destination
        # layers associated with the process

        if self.process_S.user_project.schemata == '*':

            sql = "SELECT * FROM process.schemata WHERE sub_process_id = '%(sub_process_id)s';" % {'sub_process_id':self.process_S.process.sub_process_id}

            recs = session._Execute_search_all_sql(sql)

            if len(recs) != 1:

                error_msg = '❌ ERROR: No distinct record found for sub_process_id <%s> and schemata <%s>' %(self.process_S.process.sub_process_id,
                                                                                        self.process_S.user_project.schemata)
                Log(error_msg)

                print (sql)

                return None
            
            rec = recs[0]

        else:

            sql = "SELECT * FROM process.schemata WHERE sub_process_id = '%(sub_process_id)s' AND \
                schemata = '%(schemata)s';" % {'sub_process_id':self.process_S.process.sub_process_id,
                                            'schemata':self.process_S.user_project.schemata}

            rec = session._Execute_search_single_sql(sql)

            if rec is None:

                error_msg = '          ❌ ERROR: Schemata <%s> is not defined for process <%s>' %(self.process_S.user_project.schemata,
                                                                                                    self.process_S.process.sub_process_id)
                print (error_msg)

                print ('          ❌ SQL statement: %s' %(sql))

                return None

        parameter_L = ['src_schemata', 'dst_schemata', 'src_division', 'dst_division','src_epsg',  'dst_epsg']

        value_L = [rec[2], rec[3], rec[4], rec[5], rec[6], rec[7]]

        schemata_D = dict(zip(parameter_L, value_L))
        
        self.process_S.process.schemata = Struct(schemata_D)

        # if the process is not associated with a region, return True
        if rec[2] == rec[3] == rec[4] == rec[5] == 'None':

            return True

        if hasattr(self.process_S.user_project, 'tract_name'):
            # SELECT the parent/twin region and its category via the tract defined in the user project file
            # SELECT the region_id, region_name, region_type, region_code, region_descr, region_parent
            sql = "SELECT R.name, R.id, R.category_id, R.x_min, R.y_min, R.x_max, R.y_max FROM region.region AS R \
                    LEFT JOIN region.tract as T ON (R.id = T.region_id) WHERE \
                    T.name = '%(tract)s' AND R.schemata = '%(schemata)s';"  % {'tract':self.process_S.user_project.tract_name,
                                                'schemata':self.process_S.user_project.schemata}
            
            rec = session._Execute_search_single_sql(sql)

            if rec is None:

                error_msg = '❌ ERROR: No region found for sql %s ' %(sql)
                
                print (error_msg)

                return None

        if schemata_D['src_division'] == 'region':

            self.process_S.process.locus_L.append(rec[0]) 

            self.process_S.process.locus_D[rec[0]] = {'locus':rec[0], 'path':rec[1]}     

        if schemata_D['src_division'] == 'tiles':

            error_msg = '❌ TGTODO: ADD TILES in CLASS LOCATION ' 
            
            print (error_msg)

            return None
        
        return True

    def _Set_compositions(self, session):
        '''
        '''

        # Set the dictionaries to hold the source (src) and destination (dst) compositions
        self.src_composition_D = {}; self.dst_composition_CompD = {}

        def Get_Compositions(src_dst_comp):
            '''
            '''

            json_comp_D = {}

            # Select the composition(s) from the database
            query_D = {'sub_process_id':self.process.processid, 'parent': 'process', 'element': src_dst_comp, 'paramtyp': 'element'}

            paramL = ['paramid', 'defaultvalue', 'required']

            processComps = session._MultiSearch(query_D, paramL, 'process','process_parameter')

            #SELECT * FROM process.process_parameter WHERE sub_process_id = 'region_from_coordinates' AND parent = 'process' AND element = 'srccomp' AND paramtyp = 'element';

            if len(processComps) > 0:

                if hasattr(self.process, srcdstcomp):

                    if srcdstcomp == 'srccomp':

                        compsL = self.process.srccomp

                    else:

                        compsL = self.process.dstcomp

                    if not isinstance(compsL, list):

                        exitstr = 'Either scrcomp or dstcomp is not a list'

                        exit(exitstr)

                    for jsonComp in compsL:

                        # Convert the jsonComposition from Struct to dict_items
                        dct = jsonComp.__dict__.items()

                        # Loop over the dcit_items (only contains 1 item)
                        for item in dct:

                            # convert the dict_item to an ordinary dict
                            jsonCompD[item[0]] = dict ( list(item[1].__dict__.items() ) )

            return processComps, jsonCompD

        def AssembleComp(src_dst_comp, json_comp_D, default_value, layerId):
            ''' Sub processes for assembling compostions by combining db entries and json objects
            '''

            # Set query for retrieving compositions for this process from the db
            query_D = {'sub_process_id': self.process_S.process.sub_process_id, 'parent': src_dst_comp, 'element': default_value}

            # Set the params to retrieve from the db
            parameter_L = ['parameter_id', 'parameter_type', 'required', 'default_value']

            # Retrieve all compositions for this process from the db
            composition_parameters = session._MultiSearch(query_D, parameter_L, 'process','process_parameter')

            # Convert the nested list of compositions to lists of parameters and default values
            # only include parameters that are not required by the user
            parameters = [ i[0] for i in composition_parameters if not i[2] ]

            values = [ i[3] for i in composition_parameters if not i[2] ]

            # Convert the db retrieved parameters and values to a dict
            default_D = dict( zip( parameters, values) )

            # Convert the nested list of compositions to lists of parameters and default values
            # only include parameters that are not required by the user
            parameters = [ i[0] for i in composition_parameters if i[2] ]

            values = [ i[3] for i in composition_parameters if i[2] ]

            # Convert the user required parameters and values to a dict
            required_D = dict( zip( parameters, values) )

            # If this composition is in the jsonObj, prioritize the user given json parameters
            if default_value == '*' or default_value == layerId:

                main_D = Update_dict( json_comp_D, default_D)
                #HERE I AM
            else:

                main_D = default_D

            # Check that all required parameters are given
            for key in required_D:

                if not key in main_D:

                    exitstr = 'EXITING, the required parameter %s in the process %s missing in %s for layer' %(key, self.process.processid, layerId)

                    exit ( exitstr )

            if src_dst_comp == 'src_comp':
                pass
                #self.srcCompD[layerId] = Composition(mainD, self.process.parameters, self.procsys.srcsystem, self.procsys.srcdivision, self.srcPath)

            else:
                pass
                #self.dstCompD[layerId] = Composition(mainD, self.process.parameters, self.procsys.dstsystem, self.procsys.dstdivision, self.dstPath)
                
                if hasattr(self.process.parameters,'palette') and self.process.parameters.palette:
                    
                    self.dstCompD[layerId]._SetPalette(self.process.parameters.palette, session)
                
        ''' Source compositions'''

        processComps, jsonCompD = Get_Compositions('srccomp')

        # Start processing all the required compositions

        if len(processComps) > 0:

            # if there is only one comp in the db and with default value == '*',
            if len(processComps) == 1 and processComps[0][1] == '*':

                if len(jsonCompD) > 0:

                    for compkey in jsonCompD:

                        AssembleComp('srccomp', jsonCompD[compkey], '*',compkey)

                else:

                    exitstr = 'Exiting, the process %s need at least one source composition' %(self.pp.processid)

                    exit(exitstr)
            else:

                for rc in processComps:
                    
                    if not rc[1] in jsonCompD:
                        
                        exitstr = 'EXITING - the default compositon %s missing' %(rc[1])
                    
                    #Assemble compositon
                    # Not sure why there are 3 params sent, 2 should be fine 20210329
                    AssembleComp('srccomp', jsonCompD[rc[1]], rc[0] , rc[1])

        ''' Destination compositions'''

        processComps, jsonCompD = Get_Compositions('dstcomp')

        # Loop over all compositions
        if len(processComps) > 0:

            # if there is only one comp in the db and with default value == '*',
            if len(processComps) == 1 and processComps[0][1] == '*':

                if len(jsonCompD) > 0:

                    for compkey in jsonCompD:

                        AssembleComp('dstcomp', jsonCompD[compkey], '*',compkey)

                else:
                    
                    AssembleComp('dstcomp', {}, '*', '*')

            else:

                for rc in processComps:

                    pass
                #SNULLE

    def _Set_paths(self, session):
        ''' Set source path and destination path
        '''

        def _Assemble_path(src_dst_path, path_D):
            ''' Sub processes for assembling paths by combining db entries and json objects
            '''

            # Set query for retrieving path parameters for this process from the db
            query_D = {'sub_process_id':self.process_S.process.sub_process_id, 'element': src_dst_path}

            # Set the params to retrieve from the db
            param_L = ['parameter_id', 'parameter_type', 'required', 'default_value']

            path_recs = session._Multi_search(query_D, param_L, 'process','process_parameter')

            if not path_recs:

                return None
            
            # Convert the nested list of paths to lists of parameters and default values
            # only include parameters that are not required by the user
            params = [ i[0] for i in path_recs if not i[2] ]

            values = [ i[3] for i in path_recs if not i[2] ]

            # Convert the db retrieved parameters and values to a dict
            default_path_D = dict( zip( params, values) )

            # if the path_D is only partial and some parts are given in the user_project_file
            if src_dst_path in self.default_parameter_D['process']:

                # Update the path_D with the user given parameters
                Update_dict(path_D,self.default_parameter_D['process'][src_dst_path]) 

            # If this destination path is in the jsonObj, prioritize the given parameters
            if path_D:

                Update_dict(path_D, default_path_D)

            else:

                path_D = default_path_D

            # Get all the user required parameters
            user_required_params_L = [ i[0] for i in path_recs if i[2] ]

            #values = [ i[3] for i in path_recs if i[2] ]

            # Convert the user required parameters and values to a dict
            #required_D = dict( zip( parameters, values) )
            # Check that all required parameters are given
            for key in user_required_params_L:

                if not key in path_D:

                    msg = '❌ WARNING, the required %s parameter <%s> missing for sub_process_id <%s>' %(src_dst_path, key, self.process_S.process.sub_process_id)

                    Log( msg )

                    return 'error'

            return Struct(path_D)

        ''' Source path'''

        # Get the json object list of source path
        if hasattr(self.process_S.process, 'src_path'):

            path_D = dict( list( self.process_S.process.src_path.__dict__.items() ) )  

        else:

            path_D  = {}

        self.process_S.process.src_path = _Assemble_path('src_path', path_D )

        if self.process_S.process.src_path == 'error':

            return None

        ''' Destination path'''
        # Get the json object list of destination compositions
        if hasattr(self.process_S.process, 'dst_path'):

            path_D = dict( list( self.process_S.process.dst_path.__dict__.items() ) )

        else:
            
            path_D  = {}

        self.process_S.process.dst_path = _Assemble_path('dst_path', path_D)

        if self.process_S.process.dst_path == 'error':

            return None

        if self.process_S.process.src_path != None:

            src_volume_path = SetDiskPath(self.process_S.process.src_path.volume)

            if not path.exists(src_volume_path):

                error_msg = '          ❌ WARNING source volume %s does not exist' %(self.process_S.process.src_path.volume)

                print (error_msg)

                return None
            
            self.process_S.process.src_path.path = src_volume_path
            
        if self.process_S.process.dst_path != None:

            dst_volume_path = SetDiskPath(self.process_S.process.dst_path.volume)

            if not path.exists(dst_volume_path):

                try:

                    makedirs(dst_volume_path)

                except:

                    error_msg = '          ❌ WARNING destination volume/path <%s> does not exist' %(self.process_S.process.dst_path.volume)

                    print (error_msg)

                    return None
                
            self.process_S.process.dst_path.path = dst_volume_path
            
        return True

    def _Assemble_single_process(self, p_str, process_D, json_file_FN):
        '''
        '''

        self.p_str = p_str

        Update_dict(process_D, self.default_parameter_D['process'])

        # Set the main parameters
        compiled_process_D = self.user_parameter_D

        compiled_process_D['process']  = process_D

        compiled_process_D['process']['p_str'] = p_str

        compiled_process_D['process']['json_file_FN'] = json_file_FN

        if compiled_process_D['process']['verbose'] > 2:

            print ('\n   compiled_process_D (process_center.py, 108):')

            Pprint_parameter(compiled_process_D)

        self.process_S = Struct(compiled_process_D)

    def _Assemble_parameters(self, session, process_schema='process'):
        ''' Update missing parameters from default setting
        '''

        status_OK = 1

        if not hasattr(self.process_S.process,'sub_process_id'):

            error_msg = '          ❌ WARNING process lacking sub_process_id \n \
                (file: %s;  process nr %s)' %(self.json_file_FN,
                                            self.p_str)

            Log( error_msg )

            status_OK = 0

        if not hasattr(self.process_S.process, 'parameters') or self.process_S.process.parameters == None:

            self.process_S.process.parameters = None

            error_msg = '          ❌ WARNING process lacking parameters \n \
                (file: %s;  process nr %s)' %(self.json_file_FN,
                                            self.p_str)

            Log( error_msg )

            status_OK = 0

            return status_OK

        queryD = {'sub_process_id':self.process_S.process.sub_process_id, 'parent':'process',
                  'element': 'parameters'}

        paramL =['parameter_id', 'default_value', 'required', 'parameter_type']

        param_recs = session._Multi_search(queryD, paramL, process_schema,'process_parameter')

        # Create a dict with non-required parameters
        #defaultD  = dict( [ (i[0],i[1]) for i in paramRecs if not i[2] ] )

        system_default_param_L  = [ (i[0],int( i[1] )) for i in param_recs if not i[2] and i[3].lower()[0:3] == 'int' ]

        system_default_param_L.extend([ (i[0], float( i[1] )) for i in param_recs if not i[2] and i[3].lower()[0:3] in ['flo','rea'] ] )

        system_default_param_L.extend([ (i[0], i[1]) for i in param_recs if not i[2] and i[3].lower()[0:3] not in ['int','flo','rea'] ] )

        system_default_parameter_D = dict (system_default_param_L)

        if self.default_parameter_D['process']['verbose'] > 2:

            print ('\n          system_default_parameter_D (process_center.py, 167):')

            Pprint_parameter(system_default_parameter_D)

        typeD = dict ( [ ( i[0],i[3] ) for i in param_recs ] )

        # Create a dict with compulsory parameters
        compuls_parameter_D = dict( [ (i[0],i[1]) for i in param_recs if i[2] ] )

        if self.default_parameter_D['process']['verbose'] > 2:

            print ('\n          compuls_parameter_D (process_center.py, 178):')

            Pprint_parameter(compuls_parameter_D)

        # Check that all compulsory parameters are included
        for key in compuls_parameter_D:

            if not hasattr(self.process_S.process.parameters, key):

                error_msg = '\n          ❌ WARNING compulsary parameter <%s> missing in json for process <%s>\n \
            (file: %s;  process nr: %s)' %(key,self.process_S.process.sub_process_id,
                                             self.json_file_FN,
                                            self.p_str)

                Log( error_msg)

                status_OK = 0

        # Create a process dict from process struct
        process_D = dict( list( self.process_S.process.parameters.__dict__.items() ) )

        # Update the parameters and fill in missing parameters from the system default parameters
        Update_dict(process_D, system_default_parameter_D)

        # Set the type of all params
        for p in process_D:

            if not p in typeD:

                error_msg = '\n          ❌ WARNING parameter <%s> is not defined for the \n          sub_process_id <%s>\n \
            (file: %s; process nr: %s)' %(p,self.process_S.process.sub_process_id,
                                            self.json_file_FN,
                                            self.p_str)

                Log( error_msg )

                return 0

            instance_OK = Check_param_instance(p, typeD, process_D, self.json_file_FN, self.p_str)

            if not instance_OK:

                return 0
            
        # Remove all parameters that are not in the db
        process_D = {re.sub(r"\s*\(.*?\)", "", key): value
            for key, value in process_D.items()}

        # Recreate the process struct process with the updated parameters
        self.process_S.process.parameters = Struct(process_D)

        if self.default_parameter_D['process']['verbose'] > 2:

            print ('\n   process_D (job_center.py, '
            '):')

            Pprint_parameter(process_D)

        return status_OK

    def _Set_timestep(self, timestep_C):
        ''' Set the timestep for the source and destination of this process
        '''

        if hasattr(self.process_S.process, 'period'):

            # Initiate Timestep object
            timestep_C = Timesteps()

            time_status = timestep_C._Set_initial_timesteps( self.process_S.process.period )

            if time_status:

                datum_L = timestep_C.datum_L
                datum_D = timestep_C.datum_D
                period_D = {'datum_L':datum_L, 'datum_D':datum_D}

                self.process_S.process.period = Struct(period_D)

                print ('SNULLE')

            else:

                msg = '❌ WARNING: src period and timesteps for process nr %s <%s> not ready - skipping' %(self.p_str, self.process_S.process.sub_process_id)
                
                print (msg)

                return None

        else:

            self.src_period = timestep_C

        if hasattr(self.process_S, 'dst_period'):

            # Initiate Timestep object
            timestep_C = Timesteps()

            timestep = timestep_C._Set_initial_timesteps( self.process_S.dst_period )

            if timestep:

                self.dst_period = timestep_C

            else:

                msg = '❌ WARNING: dst period and timesteps for process nr %s <%s> not ready - skipping' %(self.p_str, self.process_S.process.sub_process_id)
                
                print (msg)

                return None
        else:

            self.dst_period = timestep_C

        return True

def Project_login(default_parameter_D):
    '''
    Login to the project and set user status
    '''

    result = PG_check_user_settings(default_parameter_D)

    if not result:

        print ('❌ WARNING: could not login to database - exiting')

        return None
    
    if not 'user_project' in default_parameter_D:

        print ('❌ WARNING: object <user_project> missing in suer_project_file')

        return None

    if 'user_netrc_id' in default_parameter_D['user_project'] and \
        len(default_parameter_D['user_project']['user_netrc_id']) > 0:

        rec = PG_user_status(result, default_parameter_D['user_project']['user_netrc_id'])

    else:

        rec = PG_user_status(result,
                             None,
                             default_parameter_D['user_project']['user_id'],
                             b64encode(default_parameter_D['user_project']['user_password']).encode())
    if not rec:

        print ('❌ WARNING: unrecognised user (%s) - exiting' %(default_parameter_D['user_project']['user_id']))

        return None

    parmas = ['id', 'email', 'first_name', 'middle_name', 'last_name', 'name', 'stratum_code', 'status_code']
    #user_id, user_rights, user_region, user_project, user_project_id, user_project_region, user_project_region_id = rec

    user_status_D = dict(zip(parmas, rec))

    return user_status_D

def Get_process_from_db(pg_session_C,process_schema,process_parameter_C,user_status_D,json_file_obj, p_str):
    
    # Check if the process is in the database
    query_D = {'sub_process_id':p['sub_process_id']}

    record = pg_session_C._Single_Search(query_D, ['sub_process_id'], process_schema, 'sub_process')

    if not record:

        print ('          ❌ WARNING: sub process <%s> not in DB - skipping' %p['sub_process_id'])
        print ('          Json: %s\n          process nr: %s' %(json_file_obj, p_str))

        return None

    # Get the root process for this sub process
    query_D = {'sub_process_id':p['sub_process_id']}

    record = pg_session_C._Single_Search(query_D, ['root_process_id','min_user_stratum'], process_schema, 'sub_process')

    if not record:

        print ('          ❌ WARNING: root process id for sub process <%s> not in DB - skipping' %p['sub_process_id'])
        print ('          Json: %s\n          process nr: %s' %(json_file_obj, p_str))

        return None

    root_process_id, min_user_stratum = record

    if user_status_D['stratum_code'] < min_user_stratum:

        print ('          ❌ WARNING: user stratum too low for process %s - skipping' %p['sub_process_id'])

        return None

    process_parameter_C._Assemble_single_process(p_str, p, path.split(json_file_obj)[1])

    status_OK = process_parameter_C._Assemble_parameters(pg_session_C)

    if not status_OK:

        print ('\n          ❌ WARNING: process nr %s <%s> not ready - skipping' %(p_nr, p['sub_process_id']))

        return None

    ##### Timestep - for timeseries data with a temporal component #####
    '''
    # Initiate a Timestep object
    timestep_C = Timesteps()

    # Set the time period and frequency for the process
    if hasattr(process_parameter_C.process_S, 'period'):

        timestep = timestep_C._Set_initial_timesteps( process_parameter_C.process_S.period )

    else:

        timestep = None
    '''
    '''
    # Set timestep to the Timestep class
    timestep = timestep_C

    # Set the source and destination timesteps
    status_OK = process_parameter_C._Set_timestep(timestep)

    if not status_OK:

        print ('\n          ❌ WARNING: period and timesteps for process nr %s <%s> not ready - skipping' %(p_nr, p['sub_process_id']))

        continue
    '''

    ##### Location - for spatial data with a geographical component #####
    '''
    # Initiate a Location object

    location_C = Location(process_parameter_C.process_S, 
                            pg_session_C )
    
    # Set the time period and frequency for the process
    if hasattr(process_parameter_C.process_S, 'coordinate_system'):

        location = location_C._Set_initial_location( process_parameter_C.process_S.parameters, 
                                                    process_parameter_C.process_S.coordinate_system, 
                                                    pg_session_C )

    else:

        location = None
    
    if not location:

        print ('\n          ❌ WARNING: period and timesteps for process nr %s <%s> not ready - skipping' %(p_nr, p['sub_process_id']))

        continue

    # Set timestep to the Timestep class
    location = location_C

    # Set the source and destination timesteps
    status_OK = process_parameter_C._Set_location(location)

    if not status_OK:

        print ('\n          ❌ WARNING: location for process nr %s <%s> not ready - skipping' %(p_nr, p['sub_process_id']))

        continue
    '''

    ##### Set_paths - for processes that reads or writes data to disks  #####
    # Set the source and destination paths
    status_OK = process_parameter_C._Set_timestep(pg_session_C)

    if not status_OK:

        print ('\n          ❌ WARNING: timestep for process nr %s <%s> not ready - skipping' %(p_nr, p['sub_process_id']))

        return None

    ##### Set_paths - for processes that reads or writes data to disks  #####
    # Set the source and destination paths
    status_OK = process_parameter_C._Set_paths(pg_session_C)

    if not status_OK:

        print ('\n          ❌ WARNING: src/dst volumes for process nr %s <%s> not ready - skipping' %(p_nr, p['sub_process_id']))

        return None

    #process_parameter_C._Set_compositions(pg_session_C)

    #PP._CopyCompositions(self.session)

    #PP._SetLayers()

    status_OK = process_parameter_C._Location(pg_session_C)

    if not status_OK:

        print ('\n          ❌ WARNING: location for process nr %s <%s> not ready - skipping' %(p_nr, p['sub_process_id']))

        return None

    process_parameter_C.process_S.process.root_process_id = root_process_id

    return process_parameter_C.process_S

    #json_cmd_D[json_file_obj][p_nr] = process_parameter_C.process_S

def Job_processes_loop(default_parameter_D, user_json_process_L, process_path='', process_schema='process'):
    '''
    Setup the processes to run the project
    '''

    if (default_parameter_D['postgresdb']['db']):

        # Get user status
        user_status_D = Project_login(default_parameter_D)

        if not user_status_D:

            return None
        
        # Set user status
        default_parameter_D['user_status'] = user_status_D

        # Set verbosity for database responses
        pg_session_C = PG_session(default_parameter_D['postgresdb']['db'], default_parameter_D['process'][0]['verbose'])

    default_parameter_D['process_path'] = process_path

    verbose = default_parameter_D['process'][0]['verbose']

    #user_json_process_L = pilot_L

    # Clean the list of json objects from comments and whithe space etc
    # user_json_process_L = [path.join(process_path,x.strip())  for x in pilot_L if len(x) > 1 and x[0] != '#']

    # Set the default parameters
    process_parameter_C  = Params(default_parameter_D)

    # Loop over all json files to check if they exists
    for json_file_obj in user_json_process_L:

        if not path.exists(json_file_obj):

            error_msg = '          ❌ WARNING - json process file not found:\n    %s' %(json_file_obj)

            print (error_msg)

            if (default_parameter_D['postgresdb']['db']):

                # Close the db connection
                pg_session_C._Close()

            return None

    # Dict to hold all processes ready to run
    json_cmd_D = {}

    # Loop over all json files
    for json_file_obj in user_json_process_L:

        json_cmd_D[json_file_obj] = {}

        if verbose > 0:

            msg = '\n    reading jsonObj:\n    %s' %(json_file_obj)

            print (msg)

        user_parameter_D = Read_json(json_file_obj)

        if not user_parameter_D:

            msg = ('\n          ❌ WARNING: json file\n          %s\n          not read - skipping' %json_file_obj)

            print (msg)

            continue

        # Set the user defined parameters
        process_parameter_C._Set_user_params(user_parameter_D, path.split(json_file_obj)[1])

        # Loop over all processes in the json file
        for p_nr, p in enumerate(user_parameter_D['process']):

            p_str = str(p_nr)

            if not 'sub_process_id' in p:

                error_msg = '          ❌ WARNING: process %s missing sub_process_id' %(p_str)
                
                print (error_msg)

                continue

            if (default_parameter_D['postgresdb']['db']):

                result = Get_process_from_db(pg_session_C,process_schema,process_parameter_C,user_status_D,json_file_obj, p_str)
                
                if result:

                    json_cmd_D[json_file_obj][p_nr] = process_parameter_C.process_S
                    
            else: # No db connection, just read the parameters

                process_D = {"process_S":{"process":{ "sub_process_id": user_parameter_D['process'][p_nr]['sub_process_id'],
                                                                  "overwrite": user_parameter_D['process'][p_nr]['overwrite'],
                                                                    "parameters": user_parameter_D['process'][p_nr]['parameters']}}}

                json_cmd_D[json_file_obj][p_nr] = Struct(process_D)

    # Close the db connection if this is a database required process
    if (default_parameter_D['postgresdb']['db']):

        pg_session_C._Close()

    return json_cmd_D