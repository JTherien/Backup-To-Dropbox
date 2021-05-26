import os
import csv
import shutil
from tqdm import tqdm
import py7zr
import dropbox

from libs.config import get_config
from libs.hasher import content_hash

BLOCKSIZE = 4 * 1024 * 1024

config = get_config()
backup_list = config['backup inventory']
file_format = config['format']

# register 7zip as an archive format in shutil
shutil.register_archive_format('7z', py7zr.pack_7zarchive, description='7zip archive')

# establish a connection to the Dropbox API
dbx = dropbox.Dropbox(config['token'], timeout=None)
dropbox_files = [e.name for e in dbx.files_list_folder('/Backups/').entries]

with open(backup_list, 'r', newline='') as f:
    
    reader = csv.reader(f, delimiter=',', quotechar='"')
    
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

        archive_path = line[0].replace('%APPDATA%', os.environ['APPDATA'])

        if os.path.isdir(archive_path):
            
            print(f'Creating a {file_format} archive for: {archive_path}')
            shutil.make_archive(file_path, file_format, archive_path)
            file_size = os.path.getsize(file_path_ext)
            print(f'Finished creating {file_format} archive')
            
            # Get hash value of the local archive following Dropbox
            # sha256 hasihng guidelines
            local_hash = content_hash(file_path_ext, BLOCKSIZE)

            print(f'\nLocal hash: {local_hash}')
            print(f'Remote hash: {dropbox_content_hash}')

            if local_hash == dropbox_content_hash:
                    
                print('\nGenerated archive hash matches Dropbox hash. Skipping.')
                
            else:

                print('\nUploading to Dropbox.')
                               
                with open(file_path_ext, 'rb') as a:
                        
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
                                                       
                            # last complete block
                            dbx.files_upload_session_append_v2(upload_buffer,cursor)
                            cursor.offset = a.tell()

                            upload_buffer = a.read((file_size - a.tell()))

                            # delete existing file on Dropbox if it exists
                            # this is done immediately before upload to
                            # minimize the risk of deleting the remote copy
                            # before the local copy is ready to be uploaded
                            if file_name_ext in dropbox_files:
                    
                                print('Archive exists in Dropbox. Deleting old archive.')
                                dbx.files_delete(dropbox_path_ext)

                            # complete file upload
                            print(dbx.files_upload_session_finish(upload_buffer,cursor,commit))
                            progress.update(len(upload_buffer))

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
        if os.path.exists(file_path_ext):
            os.remove(file_path_ext)

dbx.close()
