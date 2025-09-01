'''
Created on 7 Oct 2018

@author: thomasgumbricht
'''

from os import path
import platform

def SetDiskPath(volume):
    ''' Set path dependent on operating system
    '''
        
    home = path.expanduser("~")
    
    user = path.split(home)[1]
    
    pf = platform.platform()
    
    if volume == '.':
        
        return "."
    
    if pf[0:5].lower() == 'linux':
        
        pf = 'linux' 
        
        
        if len(volume) > 0:
            
            volume = path.join('media',user,volume)
           
        else:
         
            pass
        
    elif  pf[0:6].lower() == 'darwin':
        
        pf = 'macos'
        
        
        if len(volume) > 0:

            volume = path.join('/volumes', volume)
            
    return volume
