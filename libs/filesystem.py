import os

def reset_tmp_dir(path:str = None):

    '''Deletes all contents of a temporary directiory. If the directory
    does not exist, create the directory.'''

    if os.path.isdir(path):

        for root, dirs, files in os.walk(path):
            
            for file in files:
                
                os.remove(os.path.join(root, file))

    else:

        os.makedirs(path)