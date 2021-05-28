'''
Requirements:
-   7z needs to be installed on the machine and 7z.exe needs to be 
    available in PATH
-   BitWarden needs to be installed on the machine and available in PATH
    for encryption options to work
'''

import os
import subprocess
import dropbox
import yaml
from libs.filesystem import reset_tmp_dir
from libs.upload import upload_tmp_files
from libs.upload import convert_size
from libs.bitwarden import bw_get_password

with open('config.yaml', 'r') as stream:

    config = yaml.safe_load(stream)

BLOCKSIZE = 4 * (1024 ** 2)
MAXSIZE = 2 * (1024 ** 3)

# establish a connection to the Dropbox API
dbx = dropbox.Dropbox(config['token'], timeout=None)
dropbox_files = [e.name for e in dbx.files_list_folder(config['dropbox_path']).entries]

# scan tmp folder for any volumes that didn't get uploaded to DropBox in
# the previous session
if len(os.listdir(config["temp_local_path"]) ) != 0:
    
    while True:

        resume_response = input(f'Scan {config["temp_local_path"]} for volumes ' \
            + 'not yet uploaded to DropBox? [Y: Default]/N: ').upper()

        if resume_response in {'Y', 'N', ''}:

            if resume_response in {'Y', ''}:
                
                upload_tmp_files(
                    dbx, 
                    config["temp_local_path"] + '*.7z', 
                    config["dropbox_path"], 
                    dropbox_files, 
                    BLOCKSIZE
                    )

                break
            
            else:

                break
            
        else:

            print('Please select Y or N.')

reset_tmp_dir(config['temp_local_path'])

# user input to determine if the archive should be encrypted
while True:
   
    encrypt_reponse = input('\nDo you want to encrypt the archives?\n' \
        +'Note: Encrypting files will force an upload to Dropbox as the content hash will always be different\n' \
        +'[Y: Default]/N: ').upper()

    if encrypt_reponse in {'Y', 'N', ''}:

        if encrypt_reponse in {'Y', ''}:
            
            password = bw_get_password(config['bw_encrypt_label'])

            if password == None:

                print('Error fetching password. Try again.')
        
            else:

                break

        else:

            password = None

            break

    else:

        print('Please select Y or N.')

# user input to select which directories to back-up
backup_list_usr_selection = []

for directory in config['backup_list']:

    while True:
        
        archive_response = input(f'Do you want to create and upload {directory["archive_name"]} [Y: Default]/N: ').upper()

        if archive_response in {'Y', 'N', ''}:
            
            if archive_response in {'Y', ''}:
                
                backup_list_usr_selection.append(directory)

            break

        else:

            print('Please select Y or N.')

for directory in backup_list_usr_selection:
    
    local_directory = os.path.expandvars(directory['path'])
    tmp_file = f'{config["temp_local_path"]}{directory["archive_name"]}.7z'

    if os.path.isdir(local_directory):
        
        # get the uncompressed size of the directory
        dir_size_uncompressed = 0
        
        for path, dirs, files in os.walk(local_directory):
            for f in files:
                fp = os.path.join(path, f)
                dir_size_uncompressed += os.path.getsize(fp)

        # construct the command process
        archive_process = ['7z', 'a', '-t7z', tmp_file]

        # append -mhe -p{password} to the command process if encrypted archives
        # was selected.
        if password != None:
            
            archive_process.append(f'-mhe')

            archive_process.append(f'-p{password}')

        # append the target directory to archive
        archive_process.append(local_directory)

        # if the directory size exceeds the maximum archive size, pass switches
        # to create a multi-volume archive
        if dir_size_uncompressed >= MAXSIZE:
            
            print(f'\nThe directory size ({convert_size(dir_size_uncompressed)}) '\
                + f'is larger than the {convert_size(MAXSIZE)} limit. ' \
                + 'Creating multi-volume archive.')
            
            archive_process.append(f'-v{MAXSIZE}b')

        # execute the command
        archive_result = subprocess.call(archive_process)

        # https://sevenzip.osdn.jp/chm/cmdline/exit_codes.htm
        if archive_result == 0:

            upload_tmp_files(
                dbx, 
                tmp_file, 
                config["dropbox_path"], 
                dropbox_files, 
                BLOCKSIZE
                )

        else:

            print('7z encountered an error with archiving. Skipping.')

reset_tmp_dir(config['temp_local_path'])

dbx.close()
