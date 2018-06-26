# stdlib imports
from collections import OrderedDict
import datetime
import glob
import os
import warnings

# third party imports
from amptools.io.read import read_data
from amptools.process import filter_detrend
from amptools.stream import group_channels
import numpy as np
import pandas as pd
from pgm.station_summary import StationSummary


class EventSummary(object):
    """Class for summarizing events for analysis and input into a database."""
    def __init__(self, station_dictionary):
        self._station_dict = station_dictionary

    @property
    def stations(self):
        """
        Helper method for returning a station list.

        Returns:
            list: List of station codes (str)
        """
        print("Getting list of stations.")
        stations = [station for station in self._station_dict]
        return stations

    @classmethod
    def from_files(cls, directory, imcs, imts):
        """
        Read files from a directory and return an EventSummary object.

        Args:
            directory (str): Path to input files.
            imcs (list): List of intensity measurement components (str).
            imts (list): List of intensity measurement types (str).

        Returns:
            EventSummary: EventSummary object.
        """
        streams = []
        # gather streams
        for file_path in glob.glob(directory + '/*'):
            streams += [read_data(file_path)]
        # group station traces
        streams = group_channels(streams)
        # process streams
        #TODO separate into another method and add config for processing parameters
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for idx, trace in enumerate(streams):
                streams[idx] = filter_detrend(streams[idx])
        # create dictionary of StationSummary objects
        station_dict = OrderedDict()
        for stream in streams:
            station = stream[0].stats['station']
            station_dict[station] = StationSummary(stream, imcs, imts)
        event = cls(station_dict)
        return event

    def get_station_dataframe(self, station_key):
        if station_key not in self._station_dict:
            raise KeyError('Not an available station %r.' % station_key)
        station = self._station_dict[station_key]
        pgms = station.pgms.copy()
        # Initialize dataframe headers
        dataframe_dict = OrderedDict()
        dataframe_dict[''] = []
        imt_keys = np.sort([val for val in pgms])
        imc_keys = np.sort([val for val in pgms[imt_keys[0]]])
        for imc_key in imc_keys:
            dataframe_dict[imc_key] = []
        for imt_key in imt_keys:
            dataframe_dict[''] += [imt_key]
        # Create dataframe
        for imc_key in imc_keys:
            for imt_key in imt_keys:
                dataframe_dict[imc_key] += [pgms[imt_key][imc_key]]
        # Create pandas dataframe
        dataframe = pd.DataFrame(data=dataframe_dict)
        return dataframe

    def get_flatfile_dataframe(self):
        """
        Creates a dataframe for all stations similar to a flatfile where each
        row is a channel/component.

        Returns:
            pandas.DataFrame: Flatfile-like table.
        """
        stations_obj = self._station_dict.copy()
        flat_list = []
        for station_key in self.stations:
            station = stations_obj[station_key]
            flat_rows = self.get_flatfile_row(station)
            flat_list += [flat_rows]
        all_rows = pd.concat(flat_list).reset_index(drop=True)
        return all_rows

    def get_flatfile_row(self, station):
        """
        Creates a single dataframe row similar to a flatfile where each row
        is a channel/component for one station.

        Returns:
            pandas.DataFrame: Flatfile-like table.

        Notes:
            Assumes generate_oscillators and gather_pgms has already been
            called for this class instance.
            Headers:
                - YEAR
                - MODY
                - HRMN
                - Station Name
                - Station ID  No.
                - Station Latitude
                - Station Longitude
                - Channel
                - All requested PGM values (one type per column)
                ...
        """
        pgms = station.pgms.copy()
        dataframe_dict = OrderedDict()
        # Initialize dataframe headers
        columns = ['YEAR', 'MODY', 'HRMN',
                'Station Name', 'Station ID  No.', 'Station Latitude',
                'Station Longitude', 'Channel']
        imt_keys = [val for val in pgms]
        for imt_key in np.sort(imt_keys):
            columns += [imt_key]
        for col in columns:
            dataframe_dict[col] = []
        imc_keys = [val for val in pgms[imt_keys[0]]]
        # Set metadata
        stats = station.stream[0].stats
        counter = 0
        for imc in np.sort(imc_keys):
            counter += 1
            dataframe_dict['YEAR'] += [stats['starttime'].year]
            # Format the date
            month = '{:02d}'.format(stats['starttime'].month)
            day = '{:02d}'.format(stats['starttime'].day)
            dataframe_dict['MODY'] += [month + day]
            hour = '{:02d}'.format(stats['starttime'].hour)
            minute = '{:02d}'.format(stats['starttime'].minute)
            dataframe_dict['HRMN'] += [hour + minute]
            station_str = stats['standard']['station_name']
            dataframe_dict['Station Name'] += [station_str]
            dataframe_dict['Station ID  No.'] += [stats['station']]
            latitude = stats['coordinates']['latitude']
            dataframe_dict['Station Latitude'] += [latitude]
            longitude = stats['coordinates']['longitude']
            dataframe_dict['Station Longitude'] += [longitude]
            dataframe_dict['Channel'] += [imc]
            for imt_key in np.sort(imt_keys):
                dataframe_dict[imt_key] += [pgms[imt_key][imc]]
        # Create pandas dataframe
        dataframe = pd.DataFrame(data=dataframe_dict)
        return dataframe

    def write_flatfile(self, dataframe, output_directory):
        """
        Writes the flatfile dataframe as a csv file.

        Args:
            dataframe (pandas.DataFrame): Dataframe of flatfile.
            output_directory (str): Path to output directory.
        """
        # Create file path
        today = datetime.datetime.now().strftime("%Y_%m_%d")
        filename = today + '_flatfile.csv'
        path = os.path.join(output_directory, filename)
        # Export csv
        dataframe.to_csv(path, mode = 'w', index=False)

    def write_station_table(self, dataframe, output_directory, station):
        """
        Writes the station table as a csv file.

        Args:
            dataframe (pandas.DataFrame): Dataframe of flatfile.
            output_directory (str): Path to output directory.
            station (str): Station code.
        """
        # Create file path
        filename = station + '.csv'
        path = os.path.join(output_directory, filename)
        # Export csv
        dataframe.to_csv(path, mode = 'w', index=False)
