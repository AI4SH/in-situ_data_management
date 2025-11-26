'''
Created on 16 October 2025

@author: thomasgumbricht
'''

# Third parthy imports
import numpy as np


def Interpolate_spectra(wl_L, value_L, min_wl, max_wl, step_wl, reverse=False):
    """     
    @brief Interpolates spectral data to a common wavelength grid.

    This function takes in spectral data and its corresponding wavelength array,
    and interpolates the spectra to a specified common wavelength grid defined by
    minimum wavelength, maximum wavelength, and step size.

    @param ns_array (numpy.ndarray): 2D array where each row represents a spectrum.
    @param wl_array (numpy.ndarray): 1D array of wavelengths corresponding to the spectra.
    @param min_wl (float): Minimum wavelength for the interpolation grid.
    @param max_wl (float): Maximum wavelength for the interpolation grid.
    @param step_wl (float): Step size for the interpolation grid.
    @return tuple: A tuple containing:
        - interpolated_ns_array (numpy.ndarray): 2D array of interpolated spectra.


        - common_wl_array (numpy.ndarray): 1D array of the common wavelength grid.
    """

    # Convert value_L to array

    ns_array = np.array(value_L)

    wl_in_array = np.array(wl_L)

    if reverse:
        
        ns_array = np.flip(ns_array)

        wl_in_array= np.flip(wl_in_array)

    # Create the common wavelength grid
    wl_out_array = np.arange(min_wl, max_wl + step_wl, step_wl)

    # Initialize an array to hold the interpolated spectra
    #interpolated_ns_array = np.zeros((ns_array.shape[0], len(common_wl_array)))

    # Interpolate each spectrum to the common wavelength grid
    #for i in range(ns_array.shape[0]):

    try:

        interpolated_ns_array = np.interp(wl_out_array, wl_in_array, ns_array)

    except:             

        print ('❌  ERROR - problem interpolating spectra')
        print (wl_in_array.shape, ns_array.shape)
        print (wl_in_array[0], ns_array[0])
        return None, None

    return interpolated_ns_array, wl_out_array
