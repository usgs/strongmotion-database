#!/usr/bin/env python

# stdlib imports
from collections import OrderedDict
import os

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
    assert flatfile['MODY'][0] == '0206'
    assert flatfile['MODY'][4] == '0206'
    assert flatfile['MODY'][8] == '0206'
    assert flatfile['MODY'][12] == '1113'
    assert flatfile['MODY'][18] == '0124'
    assert flatfile['Station Name'][0] == 'Anshuo'
    assert flatfile['Station Name'][4] == 'Chulu'
    assert flatfile['Station Name'][8] == 'Donghe'
    assert flatfile['Station Name'][12] == 'Te_Mara_Farm_Waiau'
    assert flatfile['Station Name'][18] == ''
    assert flatfile['Station ID  No.'][0] == 'EAS'
    assert flatfile['Station ID  No.'][4] == 'ECU'
    assert flatfile['Station ID  No.'][8] == 'EDH'
    assert flatfile['Station ID  No.'][12] == 'WTMC'
    assert flatfile['Station ID  No.'][18] == 'AOM001'
    assert flatfile['Station Latitude'][0] == 22.381
    assert flatfile['Station Latitude'][4] == 22.860
    assert flatfile['Station Latitude'][8] == 22.972
    assert flatfile['Station Latitude'][18] == 41.5267
    assert flatfile['Station Longitude'][0] == 120.857
    assert flatfile['Station Longitude'][4] == 121.092
    assert flatfile['Station Longitude'][8] == 121.305
    assert flatfile['Station Longitude'][18] == 140.9244

    try:
        event.get_station_dataframe('INVALID')
        success = True
    except KeyError:
        success = False
    assert success == False


if __name__ == '__main__':
    test_eventsummary()
