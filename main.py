import csv
import os
import hashlib
import dropbox
import shutil
import py7zr
import time
from tqdm import tqdm

from libs.config import get_config
from libs.dropbox_content_hasher import DropboxContentHasher

# future enhancements
# - tqdm async for when generating zip archive
# - Write hash values to .csv file as you go rather than waiting until the end

BLOCKSIZE = 4 * 1024 * 1024

config = get_config()
backup_list = config['backup inventory']
file_format = config['format']

# establish a connection to the Dropbox API
dbx = dropbox.Dropbox(config['token'], timeout=None)

# register 7zip as an archive format in shutil
shutil.register_archive_format('7z', py7zr.pack_7zarchive, description='7zip archive')

with open(backup_list, 'r', newline='') as f:

    reader = csv.reader(f, delimiter=',')

    lines = [['Path','Archive','Last Hash']]
    
    for line in reader:
        
        if os.path.isdir(line[0]):
            
            process_backup = True

            archive_name = f'files-to-upload\\{line[1]}'
            archive_path = f'{archive_name}.{file_format}'
            
            dropbox_path = f'/Backups/{line[1]}.{file_format}'
            
            if f'{line[1]}.{file_format}' in [e.name for e in dbx.files_list_folder('/Backups/').entries]:
                dropbox_hash = dbx.files_alpha_get_metadata(dropbox_path).content_hash
            else:
                dropbox_hash = None

            # check if dropbox_hash matches what is in the database before archiving the file
            if line[2] == dropbox_hash:
                
                print('Recorded hash matches Dropbox hash. Skipping.')
            
            else:
                
                print(f'Creating a {file_format} archive for: {line[0]}')
                shutil.make_archive(archive_name, file_format, line[0])
                archive_size = os.path.getsize(archive_path)

                print(f'Finished creating {file_format} archive')
            
                # Create a progress bar for reading the hash of the archive
                progress = tqdm(range(archive_size), 'Verifying archive changes', unit='B', unit_scale=True, unit_divisor=1024)
            
                with open(archive_path, 'rb') as a:

                    hasher = DropboxContentHasher()
                    buffer = a.read(BLOCKSIZE)
                    progress.update(len(buffer))

                    while len(buffer) > 0:
                        
                        hasher.update(buffer)
                        buffer = a.read(BLOCKSIZE)
                        progress.update(len(buffer))

                    local_hash = hasher.hexdigest()

                if local_hash == dropbox_hash:
                    
                    print('Generated archive hash matches Dropbox hash. Skipping.')
                    line[2] = local_hash
                
                else:

                    print('Uploading to Dropbox.')

                    with open(archive_path, 'rb') as a:
                        
                        # Check if the zip archive is less than 150mb
                        if archive_size <= BLOCKSIZE:

                            dbx.files_upload(a.read(), dropbox_path)

                        # read more:
                        # https://www.dropboxforum.com/t5/Dropbox-API-Support-Feedback/How-to-upload-files-in-batch/td-p/434689
                        else:
                            
                            progress = tqdm(range(archive_size), 'Uploading large file', unit='B', unit_scale=True, unit_divisor=1024)
                            
                            upload_session_start_result = dbx.files_upload_session_start(a.read(BLOCKSIZE))
                            
                            cursor = dropbox.files.UploadSessionCursor(
                                session_id=upload_session_start_result.session_id,
                                offset=a.tell()
                                )

                            commit = dropbox.files.CommitInfo(path=dropbox_path)

                            while a.tell() <= archive_size:
                                
                                upload_buffer = a.read(BLOCKSIZE)
                                
                                if ((archive_size - a.tell()) <= BLOCKSIZE):
                                    
                                    print(dbx.files_upload_session_finish(upload_buffer,cursor,commit))
                                    progress.update(len(upload_buffer))
                                    print('Upload complete')

                                    break
                            
                                else:
                                
                                    dbx.files_upload_session_append_v2(upload_buffer,cursor)
                                    progress.update(len(upload_buffer))
                                    cursor.offset = a.tell()

                        print('Updating hash value')
                        line[2] = local_hash


            # remove file after check and / or upload is complete
            os.remove(f'{archive_name}.{file_format}')

            # overwrite the backup inventory with the latest file hash
            lines.append(line)

dbx.close()

# push updated hash values to the backup inventory
with open(backup_list, 'w', newline='') as f:
    
    writer = csv.writer(f, delimiter=',')
    
    writer.writerows(lines)
