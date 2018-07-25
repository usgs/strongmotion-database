#!/usr/bin/env python

# stdlib imports
import copy
import os.path

# third party imports
import yaml

# local imports
from gmdb.constants import CONFIG_FILE, DEFAULT_CONFIG


def get_config():
    """Gets the user defined config and validates it.

    Notes:
        If no config file is present, default parameters are used.

    Returns:
        dictionary: Configuration parameters.
    """
    config_file = os.path.join(os.path.expanduser('~'), CONFIG_FILE)
    if not os.path.isfile(config_file):
        config = DEFAULT_CONFIG
    else:
        config = yaml.safe_load(open(config_file, 'rt'))
        _validate_config(config)
    return config

def _validate_config(config):
    """Helper to validate user defined config.

    Args:
        config(dictionary): Dictionary of config.

    Raises:
        TypeError if config is not a dictionary.
        KeyError if a required parameter is not included.
    """
    if not isinstance(config, dict):
        raise TypeError("Config is empty or is populated incorrectly.")
    config_keys = _get_keys(copy.deepcopy(config), [])
    default_keys = _get_keys(copy.deepcopy(DEFAULT_CONFIG), [])
    # Only look for differences in processing_parameters and pgm lists
    # The user should be able to use EventSummary for processing and analysis
    # without providing database info
    excluded_keys = ['comcat', 'host', 'scp', 'remote_host', 'keyfile', 'pdl',
            'java', 'jarfile', 'privatekey', 'configfile', 'product_source']
    difference = [key for key in default_keys if (key not in config_keys and
            key not in excluded_keys)]
    if len(difference) > 0:
        raise KeyError('Missing required parameters %r.' % difference)

def _get_keys(config_dict, keys):
    """Helper to get a list of all keys in the dictionary.

    Args:
        config_dict (dictionary): Dictionary of config.
        keys (list): list of keys (str).

    Returns:
        list: list of keys (str).
    """
    for k, v in config_dict.items():
        if isinstance(v, dict):
            _get_keys(v, keys=keys)
        else:
            keys += [k]
    return keys
