# stdlib imports
from collections import OrderedDict
import copy
import datetime
import glob
import json
import os
import warnings

# third party imports
from amptools.io.read import read_data
from amptools.process import filter_detrend
from amptools.stream import group_channels
import numpy as np
from obspy import read
from obspy.core.utcdatetime import UTCDateTime
from obspy.core.util.attribdict import AttribDict
import pandas as pd
from pgm.station_summary import StationSummary


TIMEFMT = "%Y-%m-%dT%H:%M:%S.%fZ"


class EventSummary(object):
    """Class for summarizing events for analysis and input into a database."""

    def __init__(self, station_dict):
        self._station_dict = station_dict
        self._uncorrected_streams = None
        self._corrected_streams = None

    @property
    def corrected_streams(self):
        """
        Helper method for returning a list of corrected streams.

        Returns:
            list: List of corrected streams (obspy.core.stream.Stream)
        """
        streams = copy.deepcopy(self._corrected_streams)
        return streams

    @corrected_streams.setter
    def corrected_streams(self, streams):
        """
        Helper method for setting a list of corrected streams.

        Args:
            listreamsst: List of corrected streams (obspy.core.stream.Stream)
        """
        if len(streams) == len(self.station_dict):
            self._corrected_streams = streams
        else:
            warnings.warn('Stream list is not the same length as the number '
                          'of stations. Setting failed.', Warning)

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
        # gather streams so that they can be grouped
        for file_path in glob.glob(directory + '/*'):
            streams += [read_data(file_path)]
        streams = group_channels(streams)
        uncorrected_streams = copy.deepcopy(streams)
        # TODO separate into another method and add config for processing parameters
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for idx, trace in enumerate(streams):
                streams[idx] = filter_detrend(streams[idx])
        # create dictionary of StationSummary objects for use by other methods
        station_dict = OrderedDict()
        for stream in streams:
            station = stream[0].stats['station']
            station_dict[station] = StationSummary.from_stream(stream,
                                                               imcs, imts)
        event = cls(station_dict)
        event.uncorrected_streams = uncorrected_streams
        event.corrected_streams = streams
        return event

    @classmethod
    def from_products(cls, directory, process=True):
        """
        Read obspy and geojson from a directory and return an EventSummary object.

        Args:
            directory (str): Path to input files
            process (bool): Process the streams as documented in the
                processing_parameters property of each parametric file.
                Not used at this time.

        Returns:
            EventSummary: EventSummary object.
        """
        # gather streams so that they can be grouped
        station_dict = OrderedDict()
        uncorrected_streams = []
        for file_path in glob.glob(directory + '/*'):
            if file_path.find('.json') < 0:
                stream = read(file_path)
                file_name = os.path.basename(file_path).split('.')[0]
                json_path = os.path.join(directory, file_name + '.json')
                if not os.path.isfile(json_path):
                    raise IOError('No parametric data available for '
                                  'this stream: %r. Skipping...' % file_path)
                else:
                    with open(json_path) as f:
                        parametric = json.load(f)
                    for trace in stream:
                        trace_channel = trace.stats['channel']
                        for channel in parametric['properties']['channels']:
                            chdict = parametric['properties']['channels'][channel]
                            channel_stats = chdict['stats']
                            if trace_channel == channel:
                                trace.stats = channel_stats
                    station = stream[0].stats['station']
                    pgms = parametric['properties']['pgms']
                    station_dict[station] = StationSummary.from_pgms(
                        station, pgms)
                    uncorrected_streams += [stream]
        # TODO: add processing
        event = cls(station_dict)
        event.uncorrected_streams = uncorrected_streams
        return event

    def get_channels_metadata(self, stream):
        """
        Helper method that organizes metadata for channels for get_parametric.

        Args:
            stream (obspy.core.stream.Stream): Stream of one stations data.

        Returns:
            dictionary: Channel metadata data for get_parametric.
        """
        channels = {}
        for trace in stream:
            channel = trace.stats['channel']
            # for no orientiation defined channels call channel1, channel2, or channel3
            channels_list = ['channel1', 'channel2', 'channel3']
            if channel == 'HHN' or channel == 'H1' or channel == 'N' or channel == 'S':
                channel_code = 'H1'
            elif channel == 'HHE' or channel == 'H2' or channel == 'E' or channel == 'W':
                channel_code = 'H2'
            elif channel == 'HHZ' or channel == 'Z' or channel == 'Up' or channel == 'Down':
                channel_code = 'Z'
            else:
                channel_code = channels_list.pop()
            channel_metadata = {}
            if 'processing_parameters' in trace.stats:
                channel_metadata['processing_parameters'] = copy.deepcopy(
                    trace.stats['processing'])
            stats = {}
            for key in trace.stats:
                if key != 'processing_parameters':
                    stats[key] = copy.deepcopy(trace.stats[key])
            channel_metadata['stats'] = stats
            channels[channel_code] = channel_metadata
        return channels

    def get_flatfile_dataframe(self):
        """
        Creates a dataframe for all stations similar to a flatfile where each
        row is a channel/component.

        Returns:
            pandas.DataFrame: Flatfile-like table.
        """
        stations_obj = copy.deepcopy(self.station_dict)
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
        pgms = copy.deepcopy(station.pgms)
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

    def get_parametric(self, stream):
        """
        Creates a dictionary of parametric data for one station/stream.

        Args:
            stream (obspy.core.stream.Stream): Stream of one stations data.

        Returns:
            dictionary: Parametric data for one station/stream.
        """
        # TODO add Fault and distances properties
        lon = stream[0].stats.coordinates.longitude
        lat = stream[0].stats.coordinates.latitude
        channels = self.get_channels_metadata(stream)
        station = stream[0].stats.station
        pgms = copy.deepcopy(self.station_dict[station].pgms)
        # Set properties
        properties = {}
        properties['channels'] = channels
        properties['pgms'] = pgms
        properties['process_time'] = datetime.datetime.utcnow().strftime(TIMEFMT)
        properties = self._clean_stats(properties)
        # create geojson structure
        json = {"type": "Feature",
                "geometry": {"type": "Point",
                             "coordinates": [lon, lat]},
                "properties": properties
                }
        return json

    def get_station_dataframe(self, station_key):
        """
        Create a dataframe representing imts and imcs as a tableself.

        Args:
            station_key (str): Station id.

        Returns:
            pandas.DataFrame: Table of imcs and imts.
        """
        if station_key not in self.station_dict:
            raise KeyError('Not an available station %r.' % station_key)
        station = self.station_dict[station_key]
        pgms = copy.deepcopy(station.pgms)
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

    @property
    def stations(self):
        """
        Helper method for returning a station list.

        Returns:
            list: List of station codes (str)
        """
        stations = [station for station in self.station_dict]
        return stations

    @property
    def station_dict(self):
        """
        Helper method for returning a station dictionary.

        Returns:
            dictionary: StationSummary objects for each station.
        """
        return copy.deepcopy(self._station_dict)

    @property
    def uncorrected_streams(self):
        """
        Helper method for returning a list of uncorrected streams.

        Returns:
            list: List of uncorrected streams (obspy.core.stream.Stream)
        """
        streams = copy.deepcopy(self._uncorrected_streams)
        return streams

    @uncorrected_streams.setter
    def uncorrected_streams(self, streams):
        """
        Helper method for setting a list of uncorrected streams.

        Args:
            streams (list): Uncorrected streams (obspy.core.stream.Stream)
        """
        if len(streams) == len(self.station_dict):
            self._uncorrected_streams = streams
        else:
            warnings.warn('Stream list is not the same length as the number '
                          'of stations. Setting failed.', Warning)

    def write_flatfile(self, dataframe, output_directory):
        """
        Writes the flatfile dataframe as a csv file.

        Args:
            dataframe (pandas.DataFrame): Dataframe of flatfile.
            output_directory (str): Path to output directory.
        """
        # Create file path
        today = datetime.datetime.now().strftime("%Y_%m_%d")
        filename = today + '_flatfile%s.csv'
        path = os.path.join(output_directory, filename)
        file_number = ""
        while os.path.isfile(path % file_number):
            last = path % file_number
            file_number = int(file_number or 0) + 1
        path = path % file_number
        if file_number != "":
            print('%r already exists, writing to %r.' % (last, path))
        dataframe.to_csv(path, mode='w', index=False)

    def write_parametric(self, directory):
        """
        Writes timeseries data to a specified format.

        Args:
            directory (str): Path to output directory.

        Notes:
            Obspy are listed in the documentation for the write method:
            https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html.
            Outputs will include the time series in the specified format and
            a json file containing parametric data.
        """
        # Create directory if it doesn't exist
        if not os.path.exists(directory):
            os.makedirs(directory)
        # Output parametric should be a record of the corrected streams
        # unless no corrected streams exist
        if self.corrected_streams is not None:
            streams = self.corrected_streams
        else:
            streams = self.uncorrected_streams
        for stream in streams:
            station = stream[0].stats['station']
            starttime = stream[0].stats['starttime'].strftime("%Y%m%d%H%M%S")
            extension = '.json'
            file = station + starttime + extension
            file_path = os.path.join(directory, file)
            geojson = self.get_parametric(stream)
            with open(file_path, 'wt') as f:
                json.dump(geojson, f)

    def write_station_table(self, dataframe, output_directory, station):
        """
        Writes the station table as a csv file.

        Args:
            dataframe (pandas.DataFrame): Dataframe of flatfile.
            output_directory (str): Path to output directory.
            station (str): Station code.
        """
        # Create file path
        filename = station + '%s.csv'
        path = os.path.join(output_directory, filename)
        # Export csv
        file_number = ''
        while os.path.isfile(path % file_number):
            last = path % file_number
            file_number = int(file_number or 0) + 1
        path = path % file_number
        if file_number != "":
            print('%r already exists, writing to %r.' % (last, path))
        dataframe.to_csv(path, mode='w', index=False)

    def write_timeseries(self, directory, file_format, include_json=True):
        """
        Writes timeseries data to a specified format.

        Args:
            directory (str): Path to output directory.
            file_format (str): One of the accepted obspy time series formats.
            include_json (bool): Write geojson file at the same time. Defaults
                    to True.
        Notes:
            Obspy are listed in the documentation for the write method:
            https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html.
            Outputs will include the time series in the specified format and
            a json file containing parametric data.
        """
        file_format = file_format.upper()
        # Create directory if it doesn't exist
        if not os.path.exists(directory):
            os.makedirs(directory)
        # Output streams should be a record of the uncorrected streams
        for stream in self.uncorrected_streams:
            station = stream[0].stats['station']
            starttime = stream[0].stats['starttime'].strftime("%Y%m%d%H%M%S")
            extension = '.' + file_format
            file = station + starttime + extension
            file_path = os.path.join(directory, file)
            stream.write(file_path, file_format)
        # parametric data will be required to use from_products
        if include_json is True:
            self.write_parametric(directory)

    def _clean_stats(self, stats):
        """
        Helper function for making dictionary json serializable.

        Args:
            stats (dict): Dictionary of stats.

        Returns:
            dictionary: Dictionary of cleaned stats.
        """
        for key, value in stats.items():
            if isinstance(value, (dict, AttribDict)):
                stats[key] = dict(self._clean_stats(value))
            elif isinstance(value, UTCDateTime):
                stats[key] = value.strftime(TIMEFMT)
            elif isinstance(value, float) and np.isnan(value) or value == '':
                stats[key] = 'null'
        return stats
