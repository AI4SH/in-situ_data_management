"""
process_project
==========================================

Package belonging to xspecte´s xspectrometrics Framework.

Author
------
Thomas Gumbricht (thomas.gumbricht@karttur.com)

Last update: 5 Jan 2025

"""

from .version import __version__, VERSION, metadataD

#from .process_center import Project_processes_loop

from .process_job import Job_processes_loop

#from ..import_csv_data.import_csv_data_py.json_4_Ai4SH import Json_4_AI4SH_DB_v1_xspectre, Prep_gr_penetrometer_data, \
#    CSV_to_json_for_AI4SH, Loop_data_records, Parameters_fix

from .notebook_startup import Notebook_initiate

#from .ai4sh_import_initiate import Initiate_process

__all__ = ['PGsession', 'Params', 'Notebook_initiate', 'Initiate_process']