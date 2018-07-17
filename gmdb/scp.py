# third party imports
from paramiko import SSHClient


def create_remote_folder(ssh, remote_folder):
    exists, isdir = check_remote_folder(ssh, remote_folder)
    chk_cmd1 = '[ -d %s ];echo $?' % remote_folder
    if not isdir:
        if exists:
            rm_cmd = 'rm %s' % remote_folder
            stdin, stdout, stderr = ssh.exec_command(rm_cmd)
            stdin, stdout, stderr = ssh.exec_command(chk_cmd1)
            exists = not int(stdout.read().decode('utf-8').strip())
        if not exists:
            mk_cmd = 'mkdir -p %s' % remote_folder
            stdin, stdout, stderr = ssh.exec_command(mk_cmd)
            exists, isdir = check_remote_folder(ssh, remote_folder)
            if not isdir:
                return False
    return True


def delete_remote_folder(ssh, remote_folder):
    """Check to see if remote folder exists and is a directory, then delete it.

    Args:
        ssh: SSHClient instance.
        remote_folder: Remote folder to copy local files to.

    Returns:
        bool: True indicates that remote folder existed and was deleted,
              False otherwise.
    """
    exists, isdir = check_remote_folder(ssh, remote_folder)
    chk_cmd1 = '[ -d %s ];echo $?' % remote_folder
    if isdir and exists:
        rm_cmd = 'rm -rf %s' % remote_folder
        stdin, stdout, stderr = ssh.exec_command(rm_cmd)
        stdin, stdout, stderr = ssh.exec_command(chk_cmd1)
        exists = not int(stdout.read().decode('utf-8').strip())
    else:
        return False

    return True


def check_remote_folder(ssh, remote_folder):
    """Check to see if remote folder exists and is a directory.

    Args:
        ssh: SSHClient instance.
        remote_folder: Remote folder to copy local files to.

    Returns:
        tuple: Contains two booleans -- (does a file or directory of this name
               exist, is it a directory?)
    """
    chk_cmd1 = '[ -e %s ];echo $?' % remote_folder
    stdin, stdout, stderr = ssh.exec_command(chk_cmd1)
    exists = not int(stdout.read().decode('utf-8').strip())
    chk_cmd2 = '[ -d %s ];echo $?' % remote_folder
    stdin, stdout, stderr = ssh.exec_command(chk_cmd2)
    isdir = not int(stdout.read().decode('utf-8').strip())
    return (exists, isdir)


def get_ssh_connection(remote_host, keyfile):
    ssh = SSHClient()
    # load hosts found in ~/.ssh/known_hosts
    # should we not assume that the user has these configured already?
    ssh.load_system_host_keys()
    try:
        ssh.connect(remote_host,
                    key_filename=keyfile,
                    compress=True)
    except Exception as obj:
        fmt = 'Could not connect with private key file %s: "%s"'
        raise Exception(fmt % (keyfile, str(obj)))
    return ssh
