'''
Created on 7 Oct 2018

@author: thomasgumbricht
'''

from os import path
import platform

def SetDiskPath(volume):
    """
    @brief Set disk/volume path dependent on operating system.

    This function returns a path string based on the operating system and the provided volume name.
    On Linux, it constructs a path under /media/<user>/<volume>.
    On macOS, it constructs a path under /Volumes/<volume>.
    If volume is '.', returns the current directory.

    @param volume (str): The name of the volume or '.' for current directory.
    @return str: The constructed path string based on OS and volume.
    """
        
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
