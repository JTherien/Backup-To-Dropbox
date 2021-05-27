'''
Requirements:
-   7z needs to be installed on the machine and 7z.exe needs to be 
    available in PATH
'''

import os
import subprocess
import dropbox
import yaml
from libs.hasher import content_hash
from libs.upload import upload_to_dropbox

with open('config.yaml', 'r') as stream:

    config = yaml.safe_load(stream)

dropbox_path = config['dropbox_path']
BLOCKSIZE = 4 * 1024 * 1024

# establish a connection to the Dropbox API
dbx = dropbox.Dropbox(config['token'], timeout=None)
dropbox_files = [e.name for e in dbx.files_list_folder(dropbox_path).entries]

backup_list_usr_selection = []

for directory in config['backup_list']:

    while True:
        
        response = input(f'Do you want to create and upload {directory["archive_name"]} [Y/N]: ').upper()

        if response in {'Y', 'N'}:
            
            if response == 'Y':
                
                backup_list_usr_selection.append(directory)

            break

        else:

            print('Please select Y or N.')

for directory in backup_list_usr_selection:
    
    # Dictionary used to pass all the components into the upload function at once
    local_file = {}
    local_file['local_path'] = os.path.expandvars(directory['path'])
    local_file['temp_local_path'] = config['temp_local_path']
    local_file['archive_name'] = directory["archive_name"]
    local_file['extension'] = '.7z'
    local_file['archive_name_ext'] = f'{local_file["archive_name"]}{local_file["extension"]}'
    local_file['temp_local_archive'] = f'{local_file["temp_local_path"]}{local_file["archive_name_ext"]}'
    dropbox_path_ext = f'{dropbox_path}{local_file["archive_name"]}{local_file["extension"]}'

    if local_file['archive_name_ext'] in dropbox_files:
        
        dropbox_content_hash =  dbx.files_alpha_get_metadata(dropbox_path_ext).content_hash

    else:

        dropbox_content_hash = None

    if os.path.isdir(local_file['local_path']):
            
        # Zipping to a .7z archive is faster when calling the 7z executable rather than using shutil
        # Response code generated here can also be used to validate if the archive write was a success
        archive_result = subprocess.call(['7z', 'a', '-t7z', local_file['temp_local_archive'], local_file['local_path']])

        if archive_result == 0:

            file_size = os.path.getsize(local_file['temp_local_archive'])
    
            # Get hash value of the local archive following Dropbox hasihng guidelines
            local_hash = content_hash(local_file['temp_local_archive'], BLOCKSIZE)
            print(f'\nLocal hash:\t{local_hash}')
            print(f'Remote hash:\t{dropbox_content_hash}')

            if local_hash == dropbox_content_hash:
            
                print('\nLocal and remote hashes match. Skipping upload.')
        
            else:
                        
                upload_result = upload_to_dropbox(
                    dbx, 
                    local_file,
                    dropbox_path_ext,
                    dropbox_files,
                    BLOCKSIZE
                    )

                print('Upload Complete.')
                print(f'Name: {upload_result.name}')
                print(f'Size: {upload_result.size}')
                print(f'Path: {upload_result.path_display}\n')

                if local_hash == upload_result.content_hash:
                    print('Local and remote hash values match.')
                else:
                    print('WARNING: Local and remote hash values do NOT match.')

        else:

            print('7z encountered an error with archiving. Skipping.')

    # remove file after check and / or upload is complete
    if os.path.exists(local_file['temp_local_archive']):
        os.remove(local_file['temp_local_archive'])

dbx.close()
