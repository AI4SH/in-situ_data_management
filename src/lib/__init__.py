'''
Created 12 Mar 2025
Updated 1 Sep 2025 (changed package name to lib)
Updated 8 December 2025 (removed redundant imports)

lib
==========================================

Package belonging to Kartturs AI4SH in-situ data management package.

Author
------
Thomas Gumbricht (thomas.gumbricht@karttur.com)

'''

from .spectra_2_OSSL import Interpolate_spectra

from .fix_params_coords import Parameters_fix, Coordinates_fix, Data_read

from .AI4SH_csv_data import Process_ai4sh_csv

from .xspectre_json_data import Process_xspectre_json_v089

from .NeoSpectra_csv_data import Process_neospectra_csv

from .FOSS_DS2500_csv_data import Process_ds2500_csv

from .manage_data_process import Manage_process

from .manage_data_initiate import Initiate_process

from .version import __version__, VERSION, metadataD