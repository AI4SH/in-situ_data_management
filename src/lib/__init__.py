'''
Created 12 Mar 2025
Updated 1 Sep 2025 (changed package name to lib)

lib
==========================================

Package belonging to Kartturs AI4SH in-situ data management package.

Author
------
Thomas Gumbricht (thomas.gumbricht@karttur.com)

'''
from .json_4_Ai4SH import Parameters_fix, Data_read, Loop_data_records

from .version import __version__, VERSION, metadataD

from .import_csv_data_process import Manage_process

from .import_csv_data_initiate import Initiate_process
