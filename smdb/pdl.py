# stdlib imports
import tempfile
import shutil
import json
import os.path

# third party imports
from impactutils.transfer.pdlsender import PDLSender
from libcomcat.search import get_event_by_id

PRODUCT_TYPE = 'smdb_params'
JSON_FILE = 'smdb_params.json'


def store_params(param_data, config, eventsource, eventsourcecode):
    """Store parametric data in ComCat.

    Args:
        config (dict): Dictionary containing fields:
                - java Location of Java binary.
                - jarfile Location of PDL jar file.
                - privatekey Location of PDL private key.
                - configfile Location of PDL config file.
                - product_source Network contributing this product to ComCat.
        eventsource (str): Network that originated the event.
        eventsourcecode (str): Event code from network that originated
                               the event.
    Returns:
        int: Number of files transferred (should always be 1)
        str: Message with any error information.
    """
    props = {}
    props.update(config)
    props['source'] = props['product_source']
    del props['product_source']
    props['eventsource'] = eventsource
    props['eventsourcecode'] = eventsourcecode
    props['code'] = eventsource + eventsourcecode
    props['type'] = PRODUCT_TYPE
    tdir = tempfile.mkdtemp()
    jsonfile = os.path.join(tdir, JSON_FILE)
    with open(jsonfile, 'wt') as jfile:
        json.dump(param_data, jfile)
        sender = PDLSender(properties=props, local_files=[jsonfile],
                           product_properties={'name': 'test'})
    nfiles, msg = sender.send()
    shutil.rmtree(tdir)
    return (nfiles, msg)


def get_params(eventsource, eventsourcecode, comcat_host=None):
    """Retrieve the parametric data for a given event.

    Args:
        eventsource (str): Network that originated the event.
        eventsourcecode (str): Event code from network that originated
                               the event.
        comcat_host (str): (for testing) Specify an alternate comcat host.
    Returns:
        Dictionary data structure containing parametric data for event.
    """
    eventid = eventsource + eventsourcecode
    try:
        detail = get_event_by_id(eventid, host=comcat_host)
        if not detail.hasProduct(PRODUCT_TYPE):
            return None
        sm_params = detail.getProducts(PRODUCT_TYPE)[0]
        data, url = sm_params.getContentBytes(JSON_FILE)
        jdict = json.loads(data.decode('utf8'))
        return jdict

    except Exception as e:
        raise(e)
