import hashlib
import dropbox

def content_hash(f, block_size):

    with open(f, 'rb') as a:

        hasher = hashlib.sha256()
        buffer = a.read(block_size)

        while len(buffer) > 0:
                        
            hasher.update(hashlib.sha256(buffer).digest())
            buffer = a.read(block_size)

    return hasher.hexdigest()

def get_remote_hashes(path='/Backups/', api_key=None):

    dbx = dropbox.Dropbox(api_key, timeout=None)

    dropbox_files = [e.name for e in dbx.files_list_folder(path).entries]

    return dict((x, dbx.files_alpha_get_metadata(f'{path}{x}').content_hash) for x in dropbox_files)
