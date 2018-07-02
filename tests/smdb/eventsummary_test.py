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
from smdb.event_summary import EventSummary


# stdlb import
def test_eventsummary():
    homedir = os.path.dirname(os.path.abspath(__file__))
    input_directory = os.path.join(homedir, '..', 'data')
    # test EventSummary object
    event = EventSummary.from_files(input_directory,
            ['channels', 'greater_of_two_horizontals'],
            ['pga', 'pgv', 'sa0.3', 'sa1.0', 'sa3.0'])
    assert type(event) == EventSummary
    target_stations = np.asarray(['EAS', 'ECU', 'EDH', 'WTMC', 'AOM001'])
    stations = np.asarray(event.stations)
    np.testing.assert_array_equal(np.sort(stations), np.sort(target_stations))

    # test flatfile
    flatfile = event.get_flatfile_dataframe().to_dict(into=OrderedDict)
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

    para_dict = event.get_parametric(event.corrected_streams[0])
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
            event.get_station_dataframe('INVALID')
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
    tmpdir = tempfile.mkdtemp()
    flatfile = event.get_flatfile_dataframe()
    station = event.station_dict.popitem(last=False)[0]
    print(station)
    df = event.get_station_dataframe(station)
    event.write_station_table(df, tmpdir, station)
    event.write_station_table(df, tmpdir, station)
    event.write_flatfile(flatfile, tmpdir)
    event.write_flatfile(flatfile, tmpdir)
    shutil.rmtree(tmpdir)
    tmpdir = tempfile.mkdtemp()
    event.write_timeseries(tmpdir, 'MSEED')
    prods = EventSummary.from_products(tmpdir)
    shutil.rmtree(tmpdir)
    tmpdir = tempfile.mkdtemp()
    try:
        event.write_timeseries(tmpdir, 'MSEED', False)
        prods = EventSummary.from_products(tmpdir)
        success = True
    except IOError:
        success = False
    assert success == False
    shutil.rmtree(tmpdir)


if __name__ == '__main__':
    test_eventsummary()
