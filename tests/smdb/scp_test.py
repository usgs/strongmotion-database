#!/usr/bin/env python

# stdlib imports
import os.path
from datetime import datetime

# local imports
from smdb.scp import (create_remote_folder,
                      check_remote_folder,
                      get_ssh_connection,
                      delete_remote_folder)

# third party imports
import yaml

# config file
CONFIG_FILE = '.smdb/config.yml'


def get_config():
    cfg_file = os.path.join(os.path.expanduser('~'), CONFIG_FILE)
    if not os.path.isfile(cfg_file):
        return None

    config = yaml.load(open(cfg_file))

    return config


def get_remote_cfg():
    config = get_config()
    if 'scp' not in config:
        return (None, None)

    remote_host = config['scp']['remote_host']
    keyfile = config['scp']['keyfile']

    return (remote_host, keyfile)


def test_check_remote_folder():
    remote_host, keyfile = get_remote_cfg()
    if remote_host is None:
        return True

    remote_folder = '/data'

    try:
        ssh = get_ssh_connection(remote_host, keyfile)
        print('Testing existence of %s on %s...' %
              (remote_folder, remote_host))
        exists, is_dir = check_remote_folder(ssh, remote_folder)
        assert exists
        assert is_dir
    except Exception as e:
        raise AssertionError(str(e))
    finally:
        ssh.close()


def test_ssh_connection():
    remote_host, keyfile = get_remote_cfg()
    if remote_host is None:
        return True

    try:
        print('Testing ssh connection to %s...' % remote_host)
        ssh = get_ssh_connection(remote_host, keyfile)
    except Exception as e:
        raise AssertionError(str(e))
    finally:
        ssh.close()

    assert 1 == 1


def test_create():
    remote_host, keyfile = get_remote_cfg()
    if remote_host is None:
        return True

    try:
        ssh = get_ssh_connection(remote_host, keyfile)
        nowtime = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        remote_folder = '/data/tmp_%s' % nowtime
        print('Testing creation of %s on %s...' %
              (remote_folder, remote_host))
        result = create_remote_folder(ssh, remote_folder)

        if result:
            result2 = delete_remote_folder(ssh, remote_folder)
            if not result2:
                fmt = 'Could not delete created folder "%s" on %s.'
                raise FileExistsError(fmt % (remote_folder, remote_host))
        else:
            fmt = 'Could not create folder "%s" on %s.'
            raise AssertionError(fmt % (remote_folder, remote_host))
    except Exception as e:
        raise AssertionError(str(e))
    finally:
        ssh.close()


if __name__ == '__main__':
    test_ssh_connection()
    test_check_remote_folder()
    test_create()
