import os
import subprocess

def bw_get_password(archive_label:str = None):

    proc_unlock_bw = subprocess.Popen(['bw', 'unlock'], stdout=subprocess.PIPE)

    proc_unlock_bw.wait()

    output_unlock_bw = str(proc_unlock_bw.stdout.read()).split('\\n')

    if output_unlock_bw[0] == "b'Your vault is now unlocked!":

        unlock_code = str(output_unlock_bw[3][21:-1])

        os.environ['BW_SESSION'] = unlock_code

        print('Retrieving password from BitWarden.')

        proc_get_password = subprocess.Popen(['bw', 'get', 'password', archive_label], stdout=subprocess.PIPE)

        proc_get_password.wait()

        proc_lock_bw = subprocess.Popen(['bw', 'lock'], stdout=subprocess.PIPE)

        proc_lock_bw.wait()

        print(str(proc_lock_bw.stdout.read(), 'utf-8'))

        password = str(proc_get_password.stdout.read(), 'utf-8')

        if len(password) == 0:

            return None

        else:

            return password

    else:

        return None
