#!/usr/bin/env python

# stdlib imports
import os.path
from datetime import datetime

# local imports
from gmdb.config import get_config
from gmdb.constants import DEFAULT_CONFIG
from gmdb.pdl import store_params, get_params

# third party imports
import yaml

PARAM_DICT = {'channels': {
    'H1': {'starttime': datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}}}


def test_put_params():
    config = get_config()
    if config is None or config == DEFAULT_CONFIG:
        return True
    if 'pdl' not in config:
        print('This system is not configured with a PDL client. Stopping.')
        return True
    put_params_test(config['pdl'])


def test_get_params():
    config = get_config()
    if config is None or config == DEFAULT_CONFIG:
        return True
    if 'comcat' not in config:
        fmt = 'No configuration to retrieve data from comcat. Stopping.'
        print(fmt)
        return True
    get_params_test(config['comcat']['host'])


def put_params_test(config):
    eventsource = 'us'
    eventsourcecode = '1000f114'
    nfiles, msg = store_params(PARAM_DICT, config,
                               eventsource, eventsourcecode)
    assert nfiles == 1


def get_params_test(host):
    eventsource = 'us'
    eventsourcecode = '1000f114'
    params = get_params(eventsource, eventsourcecode, comcat_host=host)
    assert params == PARAM_DICT


if __name__ == '__main__':
    test_put_params()
    test_get_params()
