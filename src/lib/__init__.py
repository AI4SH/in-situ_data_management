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

from .spectra_2_OSSL import Interpolate_spectra

from .fix_params_coords import Parameters_fix, Coordinates_fix, Data_read

from .json_4_Ai4SH import Loop_csv_data_records

from .AI4SH_4_Ai4SH import Process_ai4sh_csv

from .xspectre_4_Ai4SH import Process_xspectre_json_v089

from .NeoSpectra_4_Ai4SH import Process_neospectra_csv

from .FOSS_DS2500_4_Ai4SH import Process_ds2500_csv

from .version import __version__, VERSION, metadataD

from .import_csv_data_process import Manage_process

from .import_csv_data_initiate import Initiate_process

