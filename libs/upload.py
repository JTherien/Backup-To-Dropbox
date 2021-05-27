import dropbox
import os
import math
from tqdm import tqdm

def convert_size(size_bytes: int):
    if size_bytes == 0:
        return "0B"

    size_name = ('B', 'KB', 'MB', 'TB', 'PB', 'EB', 'ZB', 'YB')

    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)

    return f'{s}{size_name[i]}'

def upload_to_dropbox(
    dbx=None, 
    local_file:dict=None, 
    remote_file_path:str=None, 
    remote_files:list=None, 
    BLOCKSIZE: int=4*1024*1024):

    file_size = os.path.getsize(local_file['temp_local_archive'])

    progress = tqdm(
    range(file_size), 
    'Uploading to DropBox', 
    unit='B', 
    unit_scale=True, 
    unit_divisor=1024
    )

    with open(local_file['temp_local_archive'], 'rb') as a:

        upload_session_start_result = dbx.files_upload_session_start(a.read(BLOCKSIZE))
                                    
        cursor = dropbox.files.UploadSessionCursor(
            session_id=upload_session_start_result.session_id,
            offset=a.tell()
            )

        commit = dropbox.files.CommitInfo(path=remote_file_path)

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
                if local_file['archive_name_ext'] in remote_files:

                    print('\nArchive exists in Dropbox. Deleting old archive.')
                    dbx.files_delete(remote_file_path)

                # complete file upload
                result = dbx.files_upload_session_finish(upload_buffer,cursor,commit)
                progress.update(len(upload_buffer))
                progress.close()

                return result
                
            else:
                    
                dbx.files_upload_session_append_v2(upload_buffer,cursor)
                progress.update(len(upload_buffer))
                cursor.offset = a.tell()
