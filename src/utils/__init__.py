'''
Created 22 Jan 2021
Updated 12 Feb 2021
Updated4 Jan 2024

utils
==========================================

Package belonging to Kartturs AI4SH in-situ data management package.

Author
------
Thomas Gumbricht (thomas.gumbricht@karttur.com)

'''

from .version import __version__, VERSION, metadataD

from .karttur_dt import Today, Delta_days

from .setDiskPath import SetDiskPath

from .pretty_print import Pprint_parameter

from .json_read_write import Read_json, Dump_json

from .csv_read_write import Read_csv, Read_csv_excel, Write_txt_L, Write_csv_header_data

from .project_pilot import Project_pilot_locate, Root_locate, Project_locate, Get_project_path, Job_pilot_locate, Full_path_locate

from .update_dict import Update_dict

from .struct import Struct

from .list_files import Os_walk

from .code_log import Log

from .remove_diretcories_files import Remove_path
