import csv
import os
import hashlib
import dropbox
import shutil
import py7zr

from libs.config import get_config

BLOCKSIZE = 65536
CHUNKSIZE = 4 * 1024 * 1024

config = get_config()
backup_list = config['backup inventory']
file_format = config['format']
dbx = dropbox.Dropbox(config['token'])

shutil.register_archive_format('7z', py7zr.pack_7zarchive, description='7zip archive')

with open(backup_list, 'r', newline='') as f:

    reader = csv.reader(f, delimiter=',')

    lines = [['Path','Archive','Last Hash']]
    
    for line in reader:
        
        if os.path.isdir(line[0]):
            
            print(f'Creating a {file_format} archive for: {line[0]}...')
            
            archive_name = f'files-to-upload\\{line[1]}'

            archive_path = f'{archive_name}.{file_format}'

            dropbox_path = f'/Backups/{line[1]}.{file_format}'

            shutil.make_archive(archive_name, file_format, line[0])
            
            print(f'Finished creating {file_format} archive...')

            archive_size = os.path.getsize(archive_path)

            upload_file = False
            
            # Generate an SHA-1 hash of the file to check if the file
            # has was changed prior to uploading to DropBox        
            
            print(f'Checking if {file_format} archive has changed...')

            with open(archive_path, 'rb') as a:

                hasher = hashlib.sha1()
                buffer = a.read(BLOCKSIZE)
                
                while len(buffer) > 0:
                    
                    hasher.update(buffer)
                    buffer = a.read(BLOCKSIZE)
                
                if hasher.hexdigest() == line[2]:
                    
                    print(f'{file_format} archive is unchanged. Skipping upload...')
                
                else:
                
                    upload_file = True

            if upload_file:
                
                print(f'Uploading {file_format} archive to DropBox...')

                file_size = os.path.getsize(archive_path)
                
                with open(archive_path, 'rb') as a:
                    
                    # Check if the zip archive is less than 150mb
                    if archive_size <= CHUNKSIZE:

                        dbx.files_upload(a.read(), dropbox_path)

                    # https://www.dropboxforum.com/t5/Dropbox-API-Support-Feedback/How-to-upload-files-in-batch/td-p/434689
                    else:

                        upload_session_start_result = dbx.files_upload_session_start(a.read(CHUNKSIZE))

                        cursor = dropbox.files.UploadSessionCursor(
                            session_id=upload_session_start_result.session_id,
                            offset=a.tell()
                            )

                        commit = dropbox.files.CommitInfo(path=dropbox_path)

                        while a.tell() <= archive_size:
                            
                            if ((file_size - a.tell()) <= CHUNKSIZE):

                                print(dbx.files_upload_session_finish(a.read(CHUNKSIZE),cursor,commit))

                                break

                            else:
                                
                                dbx.files_upload_session_append_v2(a.read(CHUNKSIZE),cursor)

                                cursor.offset = a.tell()

                    print('Updating hash value...')
                    
                    line[2] = hasher.hexdigest()

            # remove file after check and / or upload is complete
            os.remove(f'{archive_name}.{file_format}')

            # overwrite the backup inventory with the latest file hash
            lines.append(line)

dbx.close()

# push updated hash values to the backup inventory
with open(backup_list, 'w', newline='') as f:
    
    writer = csv.writer(f, delimiter=',')
    
    writer.writerows(lines)
