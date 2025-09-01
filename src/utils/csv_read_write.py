'''
Created on 4 Jan 2024

@author: thomasgumbricht
'''

# Standard library imports

import csv  

from os import path
    

def Read_csv(FPN, mode = 'r'):

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

    with open(FPN, 'r') as csv_file:
   
        csvreader = csv.reader(csv_file, dialect='excel')

        column_L = next(csvreader)

        data_L_L = [row for row in csvreader]

    return (column_L, data_L_L)
    
def Write_txt_L(FPN, data_L):

    with open(FPN, 'w') as txt_file:
        
        for line in data_L:
            
            txt_file.write(line)




        