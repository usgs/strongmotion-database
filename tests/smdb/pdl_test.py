#!/usr/bin/env python

from smdb.pdl import store_params, get_params

# stdlib imports
import sys
import os.path

PARAM_DICT = {'channels': {
    'H1': {'starttime': '2018-01-01T12:34:56.123456'}}}


# these methods will not be called by py.test
# TODO: Figure out a way to test pdl class with Mock
# or something.


def put_test_params(config):
    eventsource = 'us'
    eventsourcecode = '1000f114'
    nfiles, msg = store_params(PARAM_DICT, config,
                               eventsource, eventsourcecode)
    assert nfiles == 1


def get_test_params(host):
    eventsource = 'us'
    eventsourcecode = '1000f114'
    params = get_params(eventsource, eventsourcecode, comcat_host=host)
    assert params == PARAM_DICT


if __name__ == '__main__':
    java = sys.argv[1]
    pdldir = sys.argv[2]
    privatekey = sys.argv[3]
    host = sys.argv[4]
    configfile = os.path.join(pdldir, 'config.ini')
    jarfile = os.path.join(pdldir, 'ProductClient.jar')
    config = {'java': java,
              'jarfile': jarfile,
              'privatekey': privatekey,
              'configfile': configfile,
              'product_source': 'us'}
    put_params(config)
    get_test_params(host)
