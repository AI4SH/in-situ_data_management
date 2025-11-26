'''
Created on 13 Jan 2023

@author: thomasgumbricht
'''

import shutil
from os import path, remove

def Remove_path(target_FP):
    """
    Remove the directory or file at the specified path.

    Parameters:
    - target_FP: Path to the directory or file to be removed.

    Returns:
    Nothing. The function will print the status of the removal.
    If the target does not exist, it will print an error message.
    If the target is a directory, it will remove it and all its contents.
    If the target is a file, it will remove the file.
    """

    if not target_FP:

        print('No target specified for removal')

        return None

    if not path.exists(target_FP):

        print('Root folder to remove does not exist:', target_FP)

        return None

    if path.isdir(target_FP):

        shutil.rmtree(target_FP)

        print('Directory removed:', target_FP)

    elif path.isfile(target_FP):

        remove(target_FP)

        print('File removed:', target_FP)

    else:

        print('Target is neither a file nor a directory:', target_FP)