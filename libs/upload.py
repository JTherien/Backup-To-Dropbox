import dropbox
import os
import math
from tqdm import tqdm
from libs.hasher import content_hash
import glob

def convert_size(size_bytes: int):
    if size_bytes == 0:
        return "0B"

    size_name = ('B','KB','MB','GB','TB','PB','EB','ZB','YB')

    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)

    return f'{s}{size_name[i]}'

def upload_to_dropbox(
    dbx=None, 
    local_file_path:str=None, 
    remote_file_path:str=None, 
    remote_files:list=None, 
    BLOCKSIZE: int=4*1024*1024):

    file_size = os.path.getsize(local_file_path)

    progress = tqdm(
    range(file_size), 
    'Uploading to DropBox', 
    unit='B', 
    unit_scale=True, 
    unit_divisor=1024
    )

    with open(local_file_path, 'rb') as a:

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
                if os.path.basename(local_file_path) in remote_files:

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

def upload_tmp_files(
    dbx=None, 
    tmp_file:str=None,
    remote_file_path:str=None,
    remote_files:list=None, 
    block_size:int=4*1024*1024):

    for volume in glob.glob(tmp_file + "*"):

        volume_base_name = os.path.basename(volume)
        dropbox_path_ext = f'{remote_file_path}{volume_base_name}'
                
        if volume_base_name in remote_files:
        
            dropbox_content_hash =  dbx.files_alpha_get_metadata(dropbox_path_ext).content_hash

        else:

            dropbox_content_hash = None

        # get hash value of the local archive following Dropbox hasihng guidelines
        local_hash = content_hash(volume, block_size)
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
                remote_files,
                block_size
                )

            print('Upload Complete.')
            print(f'Name: {upload_result.name}')
            print(f'Size: {convert_size(upload_result.size)}')
            print(f'Path: {upload_result.path_display}\n')

            if local_hash == upload_result.content_hash:
                print('Local and remote hash values match.')
            else:
                print('WARNING: Local and remote hash values do NOT match.')

    return None