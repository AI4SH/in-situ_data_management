'''
Created on 4 Jan 2024

@author: thomasgumbricht
'''

# Standard library imports

import csv  

from os import path
    

def Read_csv(FPN, mode = 'r'):

    """
    @brief Reads a CSV file and returns its header and data rows.

    @param FPN Full path name to the CSV file.
    @param mode File open mode (default is 'r').
    @return tuple (column_L, data_L_L):
        column_L: List of column headers.
        data_L_L: List of rows, each row is a list of values.
        Returns None if file does not exist.
    """
    if not path.exists(FPN):
        
        msg = 'WARNING - csv file not found:\n     %s' %(FPN)

        print (msg)
        
        return None

    with open(FPN, mode) as csv_file:
   
        csvreader = csv.reader(csv_file)

        column_L = next(csvreader)

        data_L_L = [row for row in csvreader]

    return (column_L, data_L_L)

def Read_csv_excel(FPN):

    """
    @brief Reads a CSV file using the 'excel' dialect and returns its header and data rows.

    @param FPN Full path name to the CSV file.
    @return tuple (column_L, data_L_L):
        column_L: List of column headers.
        data_L_L: List of rows, each row is a list of values.
    """
    with open(FPN, 'r') as csv_file:
        csvreader = csv.reader(csv_file, dialect='excel')
        column_L = next(csvreader)
        data_L_L = [row for row in csvreader]
    return (column_L, data_L_L)
    
def Write_txt_L(FPN, data_L):
    """
    @brief Writes a list of strings to a text file, one per line.

    @param FPN Full path name to the output text file.
    @param data_L List of strings to write to the file.
    """
    with open(FPN, 'w') as txt_file:
        for line in data_L:
            txt_file.write(line)    