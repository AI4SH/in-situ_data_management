'''
Created on 13 Jan 2023

@author: thomasgumbricht
'''

def Update_dict(main_D, default_D):
    """
    @brief Update a dictionary with default values for missing keys.

    This function updates the dictionary `main_D` by adding any keys from `default_D`
    that are not present in `main_D`. If a key exists in both, the value in `main_D` is kept.

    @param main_D dict
        The main dictionary to be updated.
    @param default_D dict
        The dictionary containing default values.
    @return None
    """

    d = {key: default_D.get(key, main_D[key]) for key in main_D}

    for key in default_D:

        if key not in d:

            main_D[key] = default_D[key]