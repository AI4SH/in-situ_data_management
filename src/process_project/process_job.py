'''
process_job.py

Module for looping jobs and processes
Created on 5 jan 2025
Updated 11 Mar 2025
Updated on 21 Aug 2025 (added option for running without DB connection)
Updated on 1 Sept 2025 (Large loop function replaced with several smaller functions,
doxygen comments added)
Updated on 8 December 2025

@author: thomasgumbricht

'''

# Standard library imports
from os import path

# Third party imports
import re

# Package application imports
from src.utils import Read_json, Pprint_parameter, SetDiskPath, Update_dict, Struct, Log

# from src.process_project import Project_login

# from src.postgres import PG_session

def Check_param_instance(p, typeD, process_D, json_file_FN, p_str):
    """
    @brief Validates the type and value of a process parameter instance against its expected type definition.

    @details
    This function performs comprehensive validation of a parameter value in a process dictionary against 
    its expected type as defined in the type dictionary. It handles multiple data types including:
    - Arrays and CSV lists
    - Text strings (with special validation for directory parameters)
    - Integers
    - Floats and real numbers
    - Booleans (with automatic string-to-boolean conversion)
    
    The function provides detailed error messages for invalid types and attempts to coerce certain 
    string representations to their correct types. For boolean parameters, it automatically converts 
    string values 'true', '1', 'false', and '0' to their boolean equivalents.
    
    Special validations:
    - Directory parameters (containing '_dir') cannot start or end with a forward slash
    - Array/CSV parameters must be either strings or lists
    - Numeric parameters are strictly validated against their declared types
    - Boolean string representations are automatically converted to proper boolean values

    @param p (str): Parameter name to validate.
    @param typeD (dict): Dictionary mapping parameter names to their expected type strings.
                        Type strings should start with one of: 'array', 'csv', 'tex', 'int', 'flo', 'rea', 'boo'.
    @param process_D (dict): Dictionary containing parameter values for the process being validated.
                            The parameter value at process_D[p] will be validated and potentially modified.
    @param json_file_FN (str): Filename of the JSON file containing the process definition, used for error reporting.
    @param p_str (str): String identifier for the process number or position, used for error reporting.

    @return bool: Returns True if the parameter instance is valid and passes all type checks.
                 Returns None if validation fails for any reason.

    @exception Prints detailed error messages to stdout when validation fails, including:
               - Parameter name and invalid value
               - JSON file name and process identifier
               - Specific validation rule that was violated
               
               Does not raise Python exceptions; returns None to indicate failure.

    @warning Directory parameters must not start or end with '/' to ensure consistent path handling.
             Boolean string conversion is case-insensitive and supports limited values ('true', '1', 'false', '0').
    """

    def _Print_error_msg(error_msg):
        """
        @brief Formats and prints an error message for an invalid process parameter instance.

        @details
        This nested helper function appends file and process location information to error messages
        to provide context about where the validation failure occurred. The formatted message includes
        the JSON file name and process identifier for easier debugging.

        @param error_msg (str): The base error message to be formatted and printed, typically describing
                               the validation failure (e.g., type mismatch, invalid value).

        @return None: Prints the formatted error message to stdout.

        @note This is a nested function only accessible within Check_param_instance.
        """

        error_msg += '              (file: %s;  process nr %s)' %(json_file_FN, p_str)

        print (error_msg)

    if 'array' in typeD[p].lower() or typeD[p].lower()[0:3] == 'csv':

        if not isinstance(process_D[p], str) and not isinstance(process_D[p], list):

            error_msg = '          ❌ ERROR parameter %s is not a list (%s)\n' %(p,process_D[p])

            _Print_error_msg(error_msg)

            return None

    elif typeD[p].lower()[0:3] == 'tex':

        if not isinstance(process_D[p], str):

            error_msg = '          ❌ ERROR parameter %s is not a string (%s)\n' %(p,process_D[p])

            _Print_error_msg(error_msg)

            return None

        if '_dir' in p.lower():

            if process_D[p][0] in ['/'] or process_D[p][len(process_D[p])-1] in ['/']:

                error_msg = '          ❌ ERROR parameter %s can not start or end with a slash (%s)\n' %(p,process_D[p])

                _Print_error_msg(error_msg)

                return None

    elif typeD[p].lower()[0:3] == 'int':

        if not isinstance(process_D[p], int) and not process_D[p].isdigit():

            error_msg = '          ❌ ERROR parameter %s is not an integer (%s)\n' %(p,process_D[p])

            _Print_error_msg(error_msg)

            return None

    elif typeD[p].lower()[0:3] == 'flo':

        if not isinstance(process_D[p], float):

            if not isinstance(process_D[p], int):

                error_msg = '          ❌ ERROR parameter %s is neither a float nor an integer (%s)\n' %(p,process_D[p])

                _Print_error_msg(error_msg)

                return None

    elif typeD[p].lower()[0:3] == 'boo':

        if not isinstance(process_D[p], bool):

            if isinstance(process_D[p], str) and process_D[p].lower() in ['true', '1']:

                process_D[p] = process_D[p].lower() == 'true'

            elif isinstance(process_D[p], str) and process_D[p].lower() in ['false','0']:

                process_D[p] = process_D[p].lower() == 'false'

            else:

                error_msg = '          ❌ ERROR parameter %s is not a boolean (%s)\n' %(p,process_D[p])

                _Print_error_msg(error_msg)

                return None

    return True

class Params():
    ''' @brief Class for extracting process parameters from user defined defaults.
    '''
    def __init__(self, default_parameter_D):
        """
        @brief Constructor for the Params class that initializes default parameters for process execution.

        @details
        This method initializes a Params object by processing the default parameters dictionary to prepare
        it for subsequent parameter assembly and validation operations. The initialization performs the
        following steps:
        
        1. **Parameter Structure Conversion**:
        - Extracts the first process configuration from the 'process' list using _Split_out_process
        - Converts the multi-process structure to a single-process format for easier access
        - This transformation allows direct dictionary access without list indexing
        
        2. **Parameter Storage**:
        - Stores the converted default parameters as an instance attribute
        - Makes default parameters available for merging with user parameters
        
        3. **Optional Verbose Output**:
        - If verbosity level is greater than 1, prints the default parameter dictionary
        - Provides debugging information during initialization
        
        The resulting default_parameter_D structure contains:
        - 'process': Single process dictionary (not a list) with default values
        - Other configuration keys: Database settings, paths, and other defaults

        @param default_parameter_D (dict): Dictionary containing the default parameters for the process.

        @return None: Constructor does not return a value but initializes instance attributes:
                    - self.default_parameter_D: Converted default parameters with single process dictionary
        """

        self.default_parameter_D = self._Split_out_process(default_parameter_D, 0)

        if self.default_parameter_D['process']['verbose'] > 1:

            print ('====== Default parameter dictionary:')

            Pprint_parameter( self.default_parameter_D )

            print ('======')

    def _Split_out_process(self, raw_D, process_nr):
        """
        @brief Extracts a single process configuration from a dictionary containing multiple processes.

        @details
        This method takes a dictionary that contains a 'process' key with a list of process configurations
        and extracts a single process at the specified index. The method creates a new dictionary where:
        - The 'process' key is replaced with the single process configuration at index process_nr
        - All other keys and their values are preserved from the original dictionary
        
        This is particularly useful for converting a multi-process configuration structure into a single-process
        structure that can be processed individually. The original dictionary remains unmodified.

        @param raw_D (dict): Dictionary containing process configurations.
                            
        @param process_nr (int): Zero-based index of the process configuration to extract from the 'process' list.

        @return dict: A new dictionary with the same structure as raw_D, except:
                    - The 'process' key now contains a single process dictionary (not a list)
                    - All other keys contain their original values from raw_D
                    Returns an empty dictionary if raw_D is empty.
        """
        process_D = {}

        for key, value in raw_D.items():

            if key == 'process':

                process_D['process'] = raw_D['process'][process_nr]

            else:

                process_D[key] = value

        return process_D

    def _Set_user_params(self,user_parameter_D,json_file_FN):
        """
    @brief Sets and merges user-defined parameters with default parameter values for process configuration.

    @details
    This method performs the following operations to prepare user parameters for process execution:
    
    1. **JSON File Registration**:
       - Stores the JSON filename for error reporting and logging purposes
    
    2. **Parameter Merging**:
       - Updates the user parameter dictionary by filling in missing variables from default parameter values
       - Uses Update_dict to recursively merge dictionaries, with user values taking precedence
       - Ensures all required parameters have either user-defined or default values
    
    3. **Instance Storage**:
       - Stores the merged parameter dictionary as an instance attribute for access by other methods
    
    The merging process ensures that:
    - User-provided parameters override default values
    - Missing user parameters are filled with default values
    - The resulting dictionary contains a complete set of parameters for process execution

    @param user_parameter_D (dict): Dictionary containing user-defined parameters for the process.
                                   Structure should match default_parameter_D format with keys like:
                                   - 'process': Process-specific configuration
                                   - Other custom parameter groups
                                   May be incomplete; missing parameters will be filled from defaults.
    @param json_file_FN (str): Filename of the JSON file containing the user parameters.
                              Used for error reporting and logging to identify the source of configuration.

    @return None: This method modifies instance attributes (self.json_file_FN and self.user_parameter_D)
                 but does not return a value.

    @note This method must be called after the Params object is initialized with default_parameter_D.
          The merged parameters are stored in self.user_parameter_D and can be accessed by subsequent
          methods like _Assemble_single_process.
          
          The Update_dict function performs a recursive merge where user values take precedence over
          default values.
    """

        self.json_file_FN = json_file_FN

        # update user_parameter_D by filling in missing variables from the default parameter values
        Update_dict(user_parameter_D, self.default_parameter_D)

        self.user_parameter_D = user_parameter_D

    def _Assemble_single_process(self, p_str, process_D, json_file_FN):
        """
        @brief Assemble and compile a single process configuration.

        This method updates the process dictionary with default parameters, sets key identifiers,
        and compiles the process configuration into a structured object for further use.

        @param p_str String identifier for the process (str).
        @param process_D Dictionary containing process-specific parameters (dict).
        @param json_file_FN Filename of the JSON file containing the process definition (str).

        The function performs the following steps:
        - Updates the process dictionary with default parameters.
        - Sets the main parameters and identifiers for the process.
        - Compiles the process configuration into a Struct object for attribute-style access.
        - Optionally prints the compiled process dictionary if verbosity is set high.
        """

        self.p_str = p_str

        Update_dict(process_D, self.default_parameter_D['process'])

        compiled_process_D = self.user_parameter_D

        compiled_process_D['process']  = process_D

        compiled_process_D['process']['p_str'] = p_str

        compiled_process_D['process']['json_file_FN'] = json_file_FN

        if compiled_process_D['process']['verbose'] > 1:

            print ('====== Complied process dictionary:')

            Pprint_parameter( compiled_process_D )

            print ('======')

        self.process_S = Struct(compiled_process_D)

    def _Assemble_parameters(self, session, process_schema='process'):
        """
        @brief Assembles and validates process parameters by merging database definitions with user-provided values.

        @details
        This method performs comprehensive parameter assembly and validation for a process by:
        
        1. **Validation Phase**:
        - Verifies the presence of required 'sub_process_id' attribute
        - Checks for existence of 'parameters' attribute in the process object
        - Returns early with error status if critical attributes are missing
        
        2. **Database Query Phase**:
        - Queries the database for parameter definitions using sub_process_id
        - Retrieves parameter metadata: parameter_id, default_value, required flag, parameter_type
        - Constructs system default parameter dictionary with type-appropriate conversions.
        
        3. **Validation Phase**:
        - Creates a dictionary of compulsory parameters that must be present
        - Validates that all required parameters exist in the user-provided parameters
        - Logs errors for any missing compulsory parameters
        
        4. **Parameter Merging Phase**:
        - Converts the process parameters structure to a dictionary
        - Merges user parameters with system defaults (user values take precedence)
        - Validates parameter types using Check_param_instance for each parameter
        
        5. **Cleanup and Finalization**:
        - Removes any extraneous text in parentheses from parameter keys
        - Recreates the process parameters as a Struct object for attribute access
        - Optionally prints the final parameter dictionary if verbosity > 1

        @param session (object): Database session object used to query parameter definitions. Must implement
                                the _Multi_search method for retrieving process parameter metadata from
                                the database schema.
        @param process_schema (str): Name of the process schema in the database where parameter definitions
                                    are stored. Defaults to 'process'. This schema should contain the
                                    'process_parameter' table with columns: parameter_id, default_value,
                                    required, parameter_type.

        @return int: Status code indicating success or failure:
                    - 1: Parameters successfully assembled and validated
                    - 0: Assembly failed due to missing required attributes, compulsory parameters,
                        or type validation errors

        @note This method requires several instance attributes to be set beforehand:
            - self.process_S.process: Process structure containing sub_process_id and parameters
            - self.default_parameter_D: Dictionary containing default parameters and verbosity settings
            - self.json_file_FN: JSON filename for error reporting
            - self.p_str: Process string identifier for error reporting
            
            The method modifies self.process_S.process.parameters in place with validated and merged parameters.

        @exception Logs error messages (does not raise exceptions) for:
                - Missing sub_process_id attribute
                - Missing or None parameters attribute
                - Missing compulsory parameters
                - Parameters not defined in database schema
                - Type validation failures for parameter values
                
                All errors are logged using the Log() function and include file name and process number context.

        @see Check_param_instance() for parameter type validation logic.
        @see Update_dict() for dictionary merging functionality.
        @see Struct() for converting dictionaries to attribute-accessible structures.
        @see Pprint_parameter() for pretty-printing parameter dictionaries.

        @warning This method assumes the database session is active and the process_schema contains
                the required 'process_parameter' table. Type validation depends on parameter_type
                values starting with specific prefixes: 'int', 'flo', 'rea', 'boo', 'tex', 'array', 'csv'.
        """

        status_OK = 1

        if not hasattr(self.process_S.process,'sub_process_id'):

            error_msg = '          ❌ ERROR process lacking sub_process_id \n \
                (file: %s;  process nr %s)' %(self.json_file_FN,
                                            self.p_str)

            Log( error_msg )

            status_OK = 0

        if not hasattr(self.process_S.process, 'parameters') or self.process_S.process.parameters == None:

            self.process_S.process.parameters = None

            error_msg = '          ❌ ERROR process lacking parameters \n \
                (file: %s;  process nr %s)' %(self.json_file_FN,
                                            self.p_str)

            Log( error_msg )

            status_OK = 0

            return status_OK

        queryD = {'sub_process_id':self.process_S.process.sub_process_id, 'parent':'process',
                  'element': 'parameters'}

        paramL =['parameter_id', 'default_value', 'required', 'parameter_type']

        param_recs = session._Multi_search(queryD, paramL, process_schema,'process_parameter')

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

                error_msg = '\n          ❌ ERROR compulsary parameter <%s> missing in json for process <%s>\n \
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

                error_msg = '\n          ❌ ERROR parameter <%s> is not defined for the \n          sub_process_id <%s>\n \
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

        if self.default_parameter_D['process']['verbose'] > 1:

            print ('\n   process_D (job_center.py, '
            '):')

            Pprint_parameter(process_D)

        return status_OK

def Get_process_from_db(pg_session_C,process_schema,process_parameter_C,user_status_D,json_process_file_obj, p, p_str):

    # Check if the process is in the database
    query_D = {'sub_process_id':p['sub_process_id']}

    record = pg_session_C._Single_Search(query_D, ['sub_process_id'], process_schema, 'sub_process')

    if not record:

        print ('          ❌ ERROR: sub process <%s> not in DB - skipping' %p['sub_process_id'])
        print ('          Json: %s\n          process nr: %s' %(json_process_file_obj, p_str))

        return None

    # Get the root process for this sub process
    query_D = {'sub_process_id':p['sub_process_id']}

    record = pg_session_C._Single_Search(query_D, ['root_process_id','min_user_stratum'], process_schema, 'sub_process')

    if not record:

        print ('          ❌ ERROR: root process id for sub process <%s> not in DB - skipping' %p['sub_process_id'])
        print ('          Json: %s\n          process nr: %s' %(json_process_file_obj, p_str))

        return None

    root_process_id, min_user_stratum = record

    if user_status_D['stratum_code'] < min_user_stratum:

        print ('          ❌ ERROR: user stratum too low for process %s - skipping' %p['sub_process_id'])

        return None

    process_parameter_C._Assemble_single_process(p_str, p, path.split(json_process_file_obj)[1])

    status_OK = process_parameter_C._Assemble_parameters(pg_session_C)

    if not status_OK:

        print ('\n          ❌ ERROR: process nr %s <%s> not ready - skipping' %(p_str, p['sub_process_id']))

        return None

    process_parameter_C.process_S.process.root_process_id = root_process_id

    return process_parameter_C.process_S

def Job_processes_loop(default_parameter_D, process_file_FPN_L, process_parameter_C, user_status_D=None,pg_session_C=None,process_schema='process'):
    """
    @brief Main loop to process job configurations from JSON files and assemble process objects.

    This function iterates over a list of JSON process files, reads user-defined parameters, and assembles process configurations
    either by retrieving details from a database or directly from the provided parameters. It supports both database-backed and
    standalone operation modes. The assembled process objects are stored in a dictionary keyed by the JSON file name and process index.

    @details
    - For each JSON file in the input list, reads the process definitions and sets user parameters.
    - For each process entry, checks for required fields and either retrieves process details from the database (if available)
    or assembles them from user parameters.
    - Handles missing fields, insufficient user privileges, and file read errors with warnings.
    - Returns a dictionary of assembled process objects ready for execution.

    @param default_parameter_D Dictionary of default parameters for the job and database connection.
    @param process_file_FPN_L List of file paths to JSON process configuration files.
    @param process_parameter_C Params object for managing process parameters and assembly.
    @param process_schema Name of the process schema in the database (default: 'process').
    @return Dictionary of assembled process objects, keyed by JSON file and process index.
    """
    def Process_loop():
        """
        @brief Loop over all processes in the JSON file and assemble process configurations.

        This function iterates through each process entry in the user parameter dictionary, checks for required fields,
        and either retrieves process details from the database or assembles them from the provided parameters. The results
        are stored in the json_cmd_D dictionary for further use.

        @details
        - Checks for the presence of 'sub_process_id' in each process entry and prints a warning if missing.
        - If a database connection is available, retrieves process details using Get_process_from_db and updates json_cmd_D.
        - If no database connection, assembles process details directly from user parameters and updates json_cmd_D.
        - Skips processes with missing required fields or insufficient user privileges.

        @note
        - Relies on global variables: user_parameter_D, default_parameter_D, pg_session_C, process_schema,
            process_parameter_C, user_status_D, json_process_file_obj, json_cmd_D.
        - Designed to be called within Job_processes_loop.

        @return None. Results are stored in json_cmd_D.
        """

        # Loop over all processes in the json process file
        for p_nr, p in enumerate(user_parameter_D['process']):

            p_str = str(p_nr)

            if not 'sub_process_id' in p:

                error_msg = '          ❌ ERROR: process %s missing sub_process_id' %(p_str)

                print (error_msg)

                continue

            # Convert all process parameters to lower case for easier matching
            #for k, v in user_parameter_D['process'][p_nr]['parameters']():
       
            #        user_parameter_D['process'][p_nr]['parameters'].update({k:v.lower()})
     

            user_parameter_D['process'][p_nr]['sub_process_id'] = user_parameter_D['process'][p_nr]['sub_process_id'].lower()

            if (default_parameter_D['postgresdb']['db']):

                result = Get_process_from_db(pg_session_C,process_schema,process_parameter_C,user_status_D,json_process_file_obj, p, p_str)

                if result:

                    json_cmd_D[json_process_file_obj][p_nr] = process_parameter_C.process_S

            else: # No db connection, just read the parameters

                process_D = {"process_S":{"process":{ "sub_process_id": user_parameter_D['process'][p_nr]['sub_process_id'],
                                                                    "overwrite": user_parameter_D['process'][p_nr]['overwrite'],
                                                                    "translate": user_parameter_D['process'][p_nr]['translate'],
                                                                    "parameters": user_parameter_D['process'][p_nr]['parameters']}}}

                # Get the project default paramters
                Update_dict(process_D['process_S']['process'], default_parameter_D['process'][0])

                json_cmd_D[json_process_file_obj][p_nr] = Struct(process_D)

    # Main loop function
    verbose = default_parameter_D['process'][0]['verbose']

    # Dict to hold all processes ready to run
    json_cmd_D = {}

    # Loop over all json files
    for json_process_file_obj in process_file_FPN_L:

        json_cmd_D[json_process_file_obj] = {}

        if verbose > 0:

            msg = '\n    reading jsonObj:\n    %s' %(json_process_file_obj)

            print (msg)

        user_parameter_D = Read_json(json_process_file_obj)

        # Set all process paramter values to lower case for easier matching
        for p_nr, p in enumerate(user_parameter_D['process']):

            for k, v in user_parameter_D['process'][p_nr]['parameters'].items():
                    
                    if isinstance(v, str):
                        
                        user_parameter_D['process'][p_nr]['parameters'].update({k:v.lower()})

            #user_parameter_D['process'][p_nr]['sub_process_id'] = user_parameter_D['process'][p_nr]['sub_process_id'].lower()
        if not user_parameter_D:

            msg = ('\n          ❌ ERROR: json file\n          %s\n          not read - skipping' %json_process_file_obj)

            print (msg)

            continue

        # Set the user defined parameters 
        process_parameter_C._Set_user_params(user_parameter_D, path.split(json_process_file_obj)[1])

        # Loop over all processes in the json file
        Process_loop()

    return json_cmd_D

def Get_set_database_session(default_parameter_D):

    """
    @brief Initializes and returns the user status and PostgreSQL database session.

    This function performs the following steps:
    - Logs in the user and retrieves their status using the provided default parameters.
    - Updates the default parameters dictionary with the user status.
    - Initializes a PostgreSQL session object with the database name and verbosity level.
    - Returns the user status dictionary and the PostgreSQL session object.

    @param default_parameter_D Dictionary containing default parameters, including database and process information.
    @return Tuple (user_status_D, pg_session_C):
        - user_status_D: Dictionary with user status information, or None if login fails.
        - pg_session_C: PostgreSQL session object, or None if login fails.
    """
    # Get user status
    user_status_D = Project_login(default_parameter_D)

    if not user_status_D:

        return None, None

    # Set user status
    default_parameter_D['user_status'] = user_status_D

    # Set verbosity for database responses
    pg_session_C = PG_session(default_parameter_D['postgresdb']['db'], default_parameter_D['process'][0]['verbose'])

    return user_status_D, pg_session_C

def Check_json_files(process_file_FPN_L):

    """
    @brief Checks the existence of JSON process files in a given list.

    This function iterates over a list of file paths and verifies that each file exists on disk.
    If any file is missing, it prints a warning message and returns None. If all files exist, it returns True.

    @param process_file_FPN_L List of file paths to JSON process configuration files.
    @return True if all files exist, None if any file is missing.
    """
    for json_process_file_obj in process_file_FPN_L:

        if not path.exists(json_process_file_obj):

            error_msg = '          ❌ ERROR - json process file not found:\n    %s' %(json_process_file_obj)

            print (error_msg)

            return None

    return True

def Structure_processes(default_parameter_D, process_file_FPN_L, process_path=''):
    """
    @brief Assemble and structure process jobs from JSON files and database parameters.

    This function checks the existence of provided JSON process files, optionally establishes a database session,
    sets up default parameters, and loops over all process files to assemble job configurations. If a database is required,
    it retrieves user status and closes the session after processing. Returns a dictionary of assembled job objects.

    @param default_parameter_D Dictionary containing default parameters for the process and database connection.
    @param process_file_FPN_L List of file paths to JSON process configuration files.
    @param process_path Optional path for process execution context (default: '').
    @return Dictionary of assembled job objects, or None if any file is missing or user/database status is invalid.
    """
    if not Check_json_files(process_file_FPN_L):

        return None

    # If this is a database required process, get the user status and open a db session
    if (default_parameter_D['postgresdb']['db']):

        # Get user status
        user_status_D, pg_session_C = Get_set_database_session(default_parameter_D)

        if not user_status_D:

            return None

    default_parameter_D['process_path'] = process_path

    # Set the default parameters
    process_parameter_C = Params(default_parameter_D)

    # Close the db connection if this is a database required process
    if (default_parameter_D['postgresdb']['db']):

        # Loop over all process files
        json_job_D = Job_processes_loop(default_parameter_D, process_file_FPN_L, process_parameter_C,user_status_D,pg_session_C)

        pg_session_C._Close()

    else:

        # Loop over all process files
        json_job_D = Job_processes_loop(default_parameter_D, process_file_FPN_L, process_parameter_C)

    return json_job_D