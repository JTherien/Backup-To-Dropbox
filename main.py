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
import glob
from libs.hasher import content_hash
from libs.upload import upload_to_dropbox
from libs.upload import convert_size
from libs.bitwarden import bw_get_password

with open('config.yaml', 'r') as stream:

    config = yaml.safe_load(stream)

BLOCKSIZE = 4 * (1024 ** 2)
MAXSIZE = 5 * (1024 ** 3)

# clear out the temporary folder
if os.path.isdir(config['temp_local_path']):

    for root, dirs, files in os.walk(config['temp_local_path']):
        for file in files:
            os.remove(os.path.join(root, file))

else:

    os.makedirs(config['temp_local_path'])

# user input to determine if the archive should be encrypted
while True:
   
    encrypt_reponse = input('Do you want to encrypt the archives\n' \
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

# establish a connection to the Dropbox API
dbx = dropbox.Dropbox(config['token'], timeout=None)
dropbox_files = [e.name for e in dbx.files_list_folder(config['dropbox_path']).entries]

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

            for volume in glob.glob(tmp_file + "*"):

                volume_base_name = os.path.basename(volume)
                volume_file_size = os.path.getsize(volume)
                dropbox_path_ext = f'{config["dropbox_path"]}{volume_base_name}'
                
                if volume_base_name in dropbox_files:
        
                    dropbox_content_hash =  dbx.files_alpha_get_metadata(dropbox_path_ext).content_hash

                else:

                    dropbox_content_hash = None
    
                # get hash value of the local archive following Dropbox hasihng guidelines
                local_hash = content_hash(volume, BLOCKSIZE)
                print(f'\n{volume_base_name}')
                print(f'Local hash:\t{local_hash}')
                print(f'Remote hash:\t{dropbox_content_hash}')

                if local_hash == dropbox_content_hash:
                
                    print('\nLocal and remote hashes match. Skipping upload.')
            
                else:
                        
                    upload_result = upload_to_dropbox(
                        dbx, 
                        volume,
                        dropbox_path_ext,
                        dropbox_files,
                        BLOCKSIZE
                        )

                    print('Upload Complete.')
                    print(f'Name: {upload_result.name}')
                    print(f'Size: {convert_size(upload_result.size)}')
                    print(f'Path: {upload_result.path_display}\n')

                    if local_hash == upload_result.content_hash:
                        print('Local and remote hash values match.')
                    else:
                        print('WARNING: Local and remote hash values do NOT match.')
            
                # remove file after check and / or upload is complete
                if os.path.exists(volume):
                    os.remove(volume)

        else:

            print('7z encountered an error with archiving. Skipping.')

dbx.close()
