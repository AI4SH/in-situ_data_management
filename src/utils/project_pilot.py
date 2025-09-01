'''
Created on 4 Jan 2024

@author: thomasgumbricht
'''

# Standard library imports

from os import path, makedirs

def Get_project_path(notebook_FP, project_path):

    if project_path.startswith('../'):

        top_path = path.split(notebook_FP)[0]

        sub_path = project_path.replace('../','')

        project_path = path.join(top_path, sub_path)

    elif project_path.startswith('.'):

        top_path = notebook_FP

        if len(project_path) > 1:

            sub_path = project_path[1:len(project_path)]

            project_path = path.join(rootpath, sub_path)

        else:

            project_path = top_path

    elif project_path.startswith('~/'):

        sub_path = project_path[2:len(project_path)]

        home_dir = path.expanduser('~')

        rootpath = home_dir

        project_path = path.join(rootpath, sub_path)

    elif project_path.startswith('~'):

        sub_path = project_path[1:len(project_path)]

        home_dir = path.expanduser('~')

        top_path = home_dir

        project_path = path.join(rootpath, top_path)

    elif project_path.startswith('/'):

        project_path = project_path

    else:

        project_path = project_path

        #msg = 'Project main path not defined correctly in:\n    %s' %(project_path)

        #print (msg)

        #return None


    if not path.exists(project_path):

        msg = 'Project main path does not exist:', project_path

        print(msg)

        project_path = None

    return project_path

def Project_locate(notebook_FP, project_json_path, default_FPN):

    if project_json_path.startswith('../'):

        rootpath = path.split(notebook_FP)[0]

        sub_path = project_json_path.replace('../','')

        project_path = path.join(rootpath, sub_path)

    elif project_json_path.startswith('.'):

        rootpath = notebook_FP

        if len(project_json_path) > 1:

            sub_path = project_json_path[1:len(project_json_path)]

            project_path = path.join(rootpath, sub_path)

        else:

            project_path = rootpath

    elif project_json_path.startswith('~'):

        sub_path = project_json_path[2:len(project_json_path)]

        home_dir = path.expanduser('~')

        rootpath = home_dir

        project_path = path.join(rootpath, sub_path)

    elif project_json_path.startswith('/'):

        project_path = project_json_path

    else:

        msg = 'Project json path not defined correctly in:\n    %s' %(default_FPN)

        print (msg)

        return None

    if not path.exists(project_path):

        msg = 'Project json path does not exist:', project_path

        print(msg)

        project_path = None

    return project_path

def Root_locate(start_FP, path_string):

    sub_path = path_string[1:len(path_string)]

    if path_string.startswith('../'):

        rootpath = path.split(start_FP)[0]

        sub_path = path_string.replace('../','')

    elif path_string.startswith('.'):

        rootpath = start_FP

    elif path_string.startswith('~'):

        sub_path = path_string[2:len(path_string)]

        home_dir = path.expanduser('~')

        rootpath = home_dir

    elif path_string.startswith('/'):

        rootpath = path_string

    else:

        exit('Root path not defined correctly in setup_db.json')

    rootpath = path.join(rootpath,sub_path)

    if not path.exists(rootpath):

        msg = 'Root path does not exist:', rootpath

        print(msg)

        rootpath = None

    return rootpath

def Doc_locate(rootpath, projPath_D):

    if 'job_folder' in projPath_D['process']:

        doc_path = path.join(rootpath, projPath_D['process']['job_folder'])

    else:

        doc_path = rootpath

    if not path.exists(doc_path):

        msg = 'Doc sub path does not exist:\n  %s' %(doc_path)

        print(msg)

        doc_path = None

    return doc_path

def Json_locate(doc_path, projPath_D):

    if 'process_sub_folder' in projPath_D['process']:

        json_path = path.join(doc_path, projPath_D['process']['process_sub_folder'])

    else:

        json_path = doc_path

    if not path.exists(json_path):

        msg = 'Json sub path does not exist:', json_path

        print(msg)

        json_path = None

    return json_path

def Project_pilot_locate(start_FP, projPath_D):

    if 'job_path' in projPath_D['process']:

        job_path  = Root_locate(start_FP, projPath_D['process']['job_path'])

    else:

        job_path  = start_FP

    if not job_path:

        return None, None, None, None, None

    doc_path = Doc_locate(job_path, projPath_D)

    if not doc_path:

        return job_path, None, None, None, None

    json_path = Json_locate(doc_path, projPath_D)

    if 'pilot_file' in projPath_D['process']:

        pilot_FPN = path.join(doc_path, projPath_D['process']['pilot_file'])

        if not path.exists(pilot_FPN):

            msg = 'Pilot file does not exist:\n %s' %(pilot_FPN)

            print(msg)

            pilot_FPN = None

    else:

        pilot_FPN = None

    return job_path, doc_path, json_path, pilot_FPN

def Job_pilot_locate(user_default_parameter_D, notebook_FP, job_path_D, job_path_def_FPN):

    if user_default_parameter_D['project_path'] == '.':

        job_path = Get_project_path(notebook_FP, job_path_D['process']['job_path'])

    elif 'job_path' in job_path_D['process']:

        job_path  = Root_locate(path.split(job_path_def_FPN)[0], job_path_D['process']['job_path'])

    else:

        job_path  = path.split(job_path_def_FPN)[0]

    if not job_path:

        return None, None, None, None, None

    doc_path = Doc_locate(job_path, job_path_D)

    if not doc_path:

        return job_path, None, None, None, None

    json_path = Json_locate(doc_path, job_path_D)

    pilot_list = None

    if not 'pilot_file' in job_path_D['process']:

        pilot_FPN = None

        if 'pilot_list' in job_path_D['process']:

            pilot_list = job_path_D['process']['pilot_list']

        else:

            pilot_list = None

            return job_path, None, None, None, None

    else:

        pilot_FPN = path.join(doc_path, job_path_D['process']['pilot_file'])

        if not path.exists(pilot_FPN):

            msg = 'Pilot file does not exist:\n %s' %(pilot_FPN)

            print(msg)

            return job_path, None, None, None, None

    return job_path, doc_path, json_path, pilot_FPN, pilot_list

def Split_up_dirpath(start_FP,path_string):
    '''
    '''

    while path_string.startswith('../'):

        start_FP = path.split(start_FP)[0]

        path_string = path_string[3:]

    return start_FP, path_string
    
def Full_path_locate(start_FP, path_string, dir_make = False):


    if path_string.startswith('../'):

        start_FP, path_string = Split_up_dirpath(start_FP,path_string)

        FPN = path.join(start_FP, path_string)

    elif path_string.startswith('./'):

        path_string = path_string[2:]

        FPN = path.join(start_FP, path_string)

    elif path_string.startswith('.'):

        path_string = path_string[1:]

        FPN = path.join(start_FP, path_string)

    elif path_string.startswith('~/'):

        path_string = path_string[2:]

        start_FP = path.expanduser('~')

        FPN = path.join(start_FP, path_string)

    elif path_string.startswith('~'):

        path_string = path_string[1:]

        start_FP = path.expanduser('~')

        FPN = path.join(start_FP, path_string)

    elif path_string.startswith('/'):

        FPN = path_string

    else:

        FPN = path.join(start_FP, path_string)

    if dir_make and not path.exists(FPN):

        makedirs(FPN)

    if not path.exists(FPN):

        msg = 'Path does not exist:\n   %s' %(FPN)

        print(msg)

        return

    return FPN
