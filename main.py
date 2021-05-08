import os
import csv
import hashlib
import shutil
from tempfile import NamedTemporaryFile
from tqdm import tqdm
import py7zr
import dropbox

from libs.config import get_config

# future enhancements
# - tqdm async for when generating zip archive
# - Write hash values to .csv file as you go rather than waiting until the end
# - Chunk excessively big libraries

BLOCKSIZE = 4 * 1024 * 1024

config = get_config()
tmp = NamedTemporaryFile('w+t', newline='', delete=False)
backup_list = config['backup inventory']
file_format = config['format']
lines = [['Path','Archive','Last Hash']]

# register 7zip as an archive format in shutil
shutil.register_archive_format('7z', py7zr.pack_7zarchive, description='7zip archive')

# establish a connection to the Dropbox API
dbx = dropbox.Dropbox(config['token'], timeout=None)
dropbox_files = [e.name for e in dbx.files_list_folder('/Backups/').entries]

with open(backup_list, 'r', newline='') as f, tmp:
    
    reader = csv.reader(f, delimiter=',', quotechar='"')
    writer = csv.writer(tmp, delimiter=',', quotechar='"')
    
    next(reader)

    for line in reader:
        
        file_name_ext = f'{line[1]}.{file_format}'
        file_path = f'files-to-upload\\{line[1]}'
        file_path_ext = f'{file_path}.{file_format}'
        dropbox_path_ext = f'/Backups/{file_name_ext}'

        if file_name_ext in dropbox_files:
            
            dropbox_content_hash =  dbx.files_alpha_get_metadata(dropbox_path_ext).content_hash

        else:

            dropbox_content_hash = None

        if (os.path.isdir(line[0])) & (line[2] != dropbox_content_hash):
            
            print(f'Creating a {file_format} archive for: {line[0]}')
            shutil.make_archive(file_path, file_format, line[0])
            file_size = os.path.getsize(file_path_ext)
            print(f'Finished creating {file_format} archive')
            
            progress = tqdm(range(file_size), 'Verifying archive changes', unit='B', unit_scale=True, unit_divisor=1024)
            
            # Get hash value of the local archive
            with open(file_path_ext, 'rb') as a:

                hasher = hashlib.sha256()
                buffer = a.read(BLOCKSIZE)
                progress.update(len(buffer))

                while len(buffer) > 0:
                        
                    hasher.update(hashlib.sha256(buffer).digest())
                    buffer = a.read(BLOCKSIZE)
                    progress.update(len(buffer))

                local_hash = hasher.hexdigest()

            # if the local archive matches Dropbox at this stage, that 
            # means the backup database had an incorrect hash value stored
            if local_hash == dropbox_content_hash:
                    
                print('Generated archive hash matches Dropbox hash. Skipping.')
                
            else:

                print('Uploading to Dropbox.')
                with open(file_name_ext, 'rb') as a:
                        
                    progress = tqdm(range(file_size), 'Uploading large file', unit='B', unit_scale=True, unit_divisor=1024)
                            
                    upload_session_start_result = dbx.files_upload_session_start(a.read(BLOCKSIZE))
                            
                    cursor = dropbox.files.UploadSessionCursor(
                        session_id=upload_session_start_result.session_id,
                        offset=a.tell()
                        )

                    commit = dropbox.files.CommitInfo(path=dropbox_path_ext)

                    while a.tell() <= file_size:
                                
                        upload_buffer = a.read(BLOCKSIZE)
                                
                        if ((file_size - a.tell()) <= BLOCKSIZE):
                                    
                            print(dbx.files_upload_session_finish(upload_buffer,cursor,commit))
                            progress.update(len(upload_buffer))
                            print('Upload complete')

                            break
                            
                        else:
                                
                            dbx.files_upload_session_append_v2(upload_buffer,cursor)
                            progress.update(len(upload_buffer))
                            cursor.offset = a.tell()

                print('Verifying hash values')

                dropbox_content_hash =  dbx.files_alpha_get_metadata(dropbox_path_ext).content_hash
                
                if local_hash == dropbox_content_hash:
                    print('Hash values verified')
                else:
                    print('Hash values do not match')
                
            line[2] = local_hash

        # remove file after check and / or upload is complete
        if os.path.isdir(file_path_ext):
            os.remove(file_path_ext)

        # overwrite the backup inventory with the latest file hash
        lines.append(line)

    for line in writer:
        
        writer.writerows(lines)

shutil.move(tmp.name, backup_list)

dbx.close()
