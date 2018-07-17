#!/usr/bin/env python

# stdlib imports
from collections import OrderedDict
import os
import shutil
import tempfile
import warnings

# third party imports
import numpy as np

# local imports
from gmdb.event_summary import EventSummary


# stdlb import
def test_eventsummary():
    homedir = os.path.dirname(os.path.abspath(__file__))
    input_directory = os.path.join(homedir, '..', 'data')
    # test EventSummary object
    event = EventSummary.fromFiles(input_directory,
            ['channels', 'greater_of_two_horizontals'],
            ['PGA', 'PGV', 'SA(0.3)', 'SA(1.0)', 'SA(3.0)'])

    # test EventSummary object from config
    event_config = EventSummary.fromFiles(input_directory)

    assert event_config.station_dict['EAS'].pgms == event.station_dict['EAS'].pgms

    assert type(event) == EventSummary
    target_stations = np.asarray(['EAS', 'ECU', 'EDH', 'WTMC', 'AOM001'])
    stations = np.asarray(event.stations)
    np.testing.assert_array_equal(np.sort(stations), np.sort(target_stations))

    # test flatfile
    flatfile = event.getFlatfileDataframe().to_dict(into=OrderedDict)
    target_modys = np.sort(np.asarray(['0206', '0206', '0206', '0206', '0206',
            '0206', '0206', '0206', '0206', '0206', '0206', '0206', '1113',
            '1113', '1113', '1113', '0124', '0124', '0124', '0124']))
    mody = flatfile['MODY']
    modys = np.sort(np.asarray([mody[key] for key in mody]))
    np.testing.assert_array_equal(modys, target_modys)
    target_names = np.sort(np.asarray(['Anshuo', 'Anshuo', 'Anshuo', 'Anshuo',
            'Chulu', 'Chulu', 'Chulu', 'Chulu', 'Donghe', 'Donghe', 'Donghe',
            'Donghe', 'Te_Mara_Farm_Waiau', 'Te_Mara_Farm_Waiau',
            'Te_Mara_Farm_Waiau', 'Te_Mara_Farm_Waiau', '', '', '', '']))
    name = flatfile['Station Name']
    names = np.sort(np.asarray([name[key] for key in name]))
    np.testing.assert_array_equal(names, target_names)
    target_ids = np.sort(np.asarray(['EAS', 'EAS', 'EAS', 'EAS', 'ECU', 'ECU',
            'ECU', 'ECU', 'EDH', 'EDH', 'EDH', 'EDH', 'WTMC', 'WTMC', 'WTMC',
            'WTMC', 'AOM001', 'AOM001', 'AOM001', 'AOM001']))
    st_id = flatfile['Station ID  No.']
    ids = np.sort(np.asarray([st_id[key] for key in st_id]))

    para_dict = event.getParametric(event.corrected_streams['AOM001'])
    target_top = np.sort(np.asarray(['type', 'geometry', 'properties']))
    top_keys = [key for key in para_dict]
    np.testing.assert_array_equal(np.sort(top_keys), target_top)
    target_properties = np.sort(np.asarray(['channels', 'process_time',
            'pgms']))
    property_keys = [key for key in para_dict['properties']]
    np.testing.assert_array_equal(np.sort(property_keys), target_properties)
    target_geometry = np.sort(np.asarray(['type', 'coordinates']))
    geometry_keys = [key for key in para_dict['geometry']]
    np.testing.assert_array_equal(np.sort(geometry_keys), target_geometry)
    target_channel = np.asarray(['stats'])
    channel_keys = [key for key in para_dict['properties']['channels']['H1']]
    np.testing.assert_array_equal(np.sort(channel_keys), target_channel)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            event.getStationDataframe('INVALID')
            success = True
        except KeyError:
            success = False
        assert success == False
        length_before = len(event.corrected_streams)
        event.corrected_streams = []
        length_after = len(event.corrected_streams)
        assert length_after == length_before
        length_before = len(event.uncorrected_streams)
        event.uncorrected_streams = []
        length_after = len(event.uncorrected_streams)
        assert length_after == length_before
        event.process(station='INVALID')

    # Test reprocessing station
    event.process(station='WTMC')

    tmpdir = tempfile.mkdtemp()
    flatfile = event.getFlatfileDataframe()
    station = event.station_dict.popitem(last=False)[0]
    df = event.getStationDataframe(station)
    event.writeStationTable(df, tmpdir, station)
    event.writeStationTable(df, tmpdir, station)
    event.writeFlatfile(flatfile, tmpdir)
    event.writeFlatfile(flatfile, tmpdir)
    shutil.rmtree(tmpdir)

    tmpdir = tempfile.mkdtemp()
    event.writeTimeseries(tmpdir, 'MSEED')
    prods = EventSummary.fromProducts(tmpdir)
    shutil.rmtree(tmpdir)

    # FileNotFoundError with missing parametric data
    tmpdir = tempfile.mkdtemp()
    try:
        event.writeTimeseries(tmpdir, 'MSEED', False)
        prods = EventSummary.fromProducts(tmpdir)
        success = True
    except FileNotFoundError:
        success = False
    assert success == False
    shutil.rmtree(tmpdir)

    # Exception with missing uncorrected_streams
    empty_event = EventSummary()
    try:
        empty_event.process()
        success = True
    except Exception:
        success = False
    assert success == False

    # Exception with missing processed_streams
    try:
        empty_event.setStationDictionary()
        success = True
    except Exception:
        success = False
    assert success == False

    # Exception with missing processed_streams
    try:
        empty_event.process(station='INVALID')
        success = True
    except Exception:
        success = False
    assert success == False

    # Exception with missing station_dict
    try:
        empty_event.getStationDataframe()
        success = True
    except Exception:
        success = False
    assert success == False

    # Exception with missing station_dict
    try:
        empty_event.getFlatfileDataframe()
        success = True
    except Exception:
        success = False
    assert success == False


if __name__ == '__main__':
    test_eventsummary()
