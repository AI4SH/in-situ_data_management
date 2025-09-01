'''
Created on 7 Oct 2018

@author: thomasgumbricht
'''

from string import whitespace

def Check_white_space(s):
    '''
    '''
    return True in [c in s for c in whitespace]

def Check_underscore(s):
    '''
    '''
    return True in [c in s for c in '_']