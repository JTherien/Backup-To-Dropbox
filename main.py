import csv
import os
from shutil import make_archive
import hashlib
import dropbox

from libs.config import get_config

config = get_config()

BLOCKSIZE = 65536
backup_list = config['backup inventory']
dbx = dropbox.Dropbox(config['token'])

# ToDo
# Feed a .csv to list the directories to include in the backup
with open(backup_list, 'r', newline='') as f:

    reader = csv.reader(f, delimiter=',')

    lines = [['Path','Archive','Last Hash']]
    
    for line in reader:
        
        if os.path.isdir(line[0]):
            
            print(f'Creating backup archive for: {line[0]}...')
            
            archive_name = f'files-to-upload\\{line[1]}'

            make_archive(archive_name, 'zip', line[0])

            # Generate an SHA-1 hash of the file to check if the file
            # has was changed prior to uploading to DropBox
            
            print('Chechking if .zip archive has changed...')

            with open(f'{archive_name}.zip', 'rb') as a:
                
                hasher = hashlib.sha1()
                buffer = a.read(BLOCKSIZE)
                
                while len(buffer) > 0:
                    
                    hasher.update(buffer)
                    buffer = a.read(BLOCKSIZE)
                
                if hasher.hexdigest() != line[2]:

                    print('Uploading archive to DropBox...')
                    
                    dbx.files_upload(a.read(), f'/Backups/{line[1]}.zip')

                    print('Updating hash value...')
                    
                    line[2] = hasher.hexdigest()
                
                else:

                    print('.zip archive is unchanged. Skipping upload...')

            os.remove(f'{archive_name}.zip')

            lines.append(line)

with open(backup_list, 'w', newline='') as f:
    writer = csv.writer(f, delimiter=',')
    writer.writerows(lines)
