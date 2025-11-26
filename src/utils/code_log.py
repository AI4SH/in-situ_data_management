'''
Created on 7 Oct 2018

@author: thomasgumbricht
'''
import inspect

def Log(message, space='          ', error=False):
    '''  Log message to the console with the file name, method name, and line number.
    '''
    
    func = inspect.currentframe().f_back.f_code
    frame = inspect.currentframe().f_back
    method = func.co_name
    if  method == '<module>':
        method = 'main'
    parts = func.co_filename.rsplit('/', 1),
    final_msg = "\n%s%s %s line %i:\n%s%s" % (
        space,
        func.co_filename.split('/')[-1],
        method,
        frame.f_lineno,
        space,
        message
    )
    print(final_msg)
    file_url = '%s#L%i' % (func.co_filename, frame.f_lineno)
    print (space, file_url)
  