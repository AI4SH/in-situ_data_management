'''
Created on 13 Jan 2023

@author: thomasgumbricht
'''

def Update_dict(main_D, default_D):
    '''
    '''

    d = {key: default_D.get(key, main_D[key]) for key in main_D}

    for key in default_D:

        if key not in d:

            main_D[key] = default_D[key]