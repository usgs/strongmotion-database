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

# local imports
from smdb.config import get_config
from smdb.constants import TIMEFMT


class EventSummary(object):
    """Class for summarizing events for analysis and input into a database."""
    def __init__(self):
        self._station_dict = None
        self._uncorrected_streams = None
        self._corrected_streams = None

    @property
    def corrected_streams(self):
        """
        Helper method for returning a dictionary of corrected streams.

        Returns:
            dictionary: Corrected streams (obspy.core.stream.Stream)

        """
        streams = copy.deepcopy(self._corrected_streams)
        return streams

    @corrected_streams.setter
    def corrected_streams(self, streams):
        """
        Helper method for setting a dictionary of corrected streams.

        Args:
            streams (dictionary): Corrected streams (obspy.core.stream.Stream)
        """
        if self.station_dict is None or len(streams) == len(self.station_dict):
            self._corrected_streams = streams
        else:
            warnings.warn('Stream dictionary is not the same length as the '
                    'number of stations. Setting failed.', Warning)

    @classmethod
    def fromFiles(cls, directory, imcs=None, imts=None):
        """
        Read files from a directory and return an EventSummary object.

        Args:
            directory (str): Path to input files.
            imcs (list): List of intensity measurement components (str). Default
                    is None.
            imts (list): List of intensity measurement types (str). Default
                    is None.

        Returns:
            EventSummary: EventSummary object.
        """
        streams = []
        # gather streams so that they can be grouped
        for file_path in glob.glob(directory + '/*'):
            streams += [read_data(file_path)]
        streams = group_channels(streams)
        uncorrected_streams = {}
        for stream in streams:
            station = stream[0].stats['station']
            uncorrected_streams[station] = stream
        event = cls()
        event.uncorrected_streams = uncorrected_streams
        event.process()
        # create dictionary of StationSummary objects for use by other methods
        event.setStationDictionary(imcs, imts)
        return event

    @classmethod
    def fromProducts(cls, directory, process=True):
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
        uncorrected_streams = {}
        for file_path in glob.glob(directory + '/*'):
            if file_path.find('.json') < 0:
                stream = read(file_path)
                file_name = os.path.basename(file_path).split('.')[0]
                json_path = os.path.join(directory, file_name + '.json')
                if not os.path.isfile(json_path):
                    raise FileNotFoundError('No parametric data available for '
                            'this stream: %r.' % file_path)
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
                uncorrected_streams[station] = stream
        ## TODO: add processing option after next amptools release
        event = cls()
        event._station_dict = station_dict
        event.uncorrected_streams = uncorrected_streams
        return event

    def getChannelsMetadata(self, stream):
        """
        Helper method that organizes metadata for channels for getParametric.

        Args:
            stream (obspy.core.stream.Stream): Stream of one stations data.

        Returns:
            dictionary: Channel metadata data for getParametric.
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
                channel_metadata['processing_parameters'] = copy.deepcopy(trace.stats['processing'])
            stats = {}
            for key in trace.stats:
                if key != 'processing_parameters':
                    stats[key] = copy.deepcopy(trace.stats[key])
            channel_metadata['stats'] = stats
            channels[channel_code] = channel_metadata
        return channels

    def getFlatfileDataframe(self):
        """
        Creates a dataframe for all stations similar to a flatfile where each
        row is a channel/component.

        Returns:
            pandas.DataFrame: Flatfile-like table.

        Raises:
            Exception if no station dictionary has been set.
        """
        if self.station_dict is None:
            raise Exception('The station dictionary has not been set. Use '
                'setStationDictionary.')
        stations_obj = copy.deepcopy(self.station_dict)
        flat_list = []
        for station_key in self.stations:
            station = stations_obj[station_key]
            flat_rows = self.getFlatfileRow(station)
            flat_list += [flat_rows]
        all_rows = pd.concat(flat_list).reset_index(drop=True)
        return all_rows

    def getFlatfileRow(self, station):
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

    def getParametric(self, stream):
        """
        Creates a dictionary of parametric data for one station/stream.

        Args:
            stream (obspy.core.stream.Stream): Stream of one stations data.

        Returns:
            dictionary: Parametric data for one station/stream.
        """
        #TODO add Fault and distances properties
        lon = stream[0].stats.coordinates.longitude
        lat = stream[0].stats.coordinates.latitude
        channels = self.getChannelsMetadata(stream)
        station = stream[0].stats.station
        pgms = copy.deepcopy(self.station_dict[station].pgms)
        # Set properties
        properties = {}
        properties['channels'] = channels
        properties['pgms'] = pgms
        properties['process_time'] = datetime.datetime.utcnow().strftime(TIMEFMT)
        properties = self._cleanStats(properties)
        # create geojson structure
        json = {"type": "Feature",
                "geometry": {"type": "Point",
                             "coordinates": [lon, lat]},
                     "properties": properties
                }
        return json

    def getStationDataframe(self, station_key):
        """
        Create a dataframe representing imts and imcs as a tableself.

        Args:
            station_key (str): Station id.

        Returns:
            pandas.DataFrame: Table of imcs and imts.

        Raises:
            Exception: If no station_dictionary is set.
            KeyError: If the station is not in the station dictionaryself.
        """
        if self.station_dict is None:
            raise Exception('The station dictionary has not been set. Use '
                'setStationDictionary.')
        elif station_key not in self.station_dict:
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

    def setStationDictionary(self, imcs=None, imts=None):
        """
        Calculate the station summaries and set the dictionary.

        Args:
            imcs (list): List of intensity measurement components (str). Default
                    is None. If none imclist from config is used.
            imts (list): List of intensity measurement types (str). Default
                    is None. If None imtlist from config is used.

        Notes:
            Requires that corrected_streams is set.

        Raises:
            Exception: If corrected_streams is not set.
        """
        # use defaults if imcs or imts are not specified
        config = get_config()
        if imcs is None:
            imcs = config['imclist']
        if imts is None:
            imts = config['imtlist']

        if self.corrected_streams is None:
            raise Exception('Processed streams are required to create a '
                    'StationSummary object and create the dictionary.')
        station_dict = OrderedDict()
        for station in self.corrected_streams:
            stream = self.corrected_streams[station]
            station_dict[station] = StationSummary.from_stream(stream,
                    imcs, imts)
        self._station_dict = station_dict

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

    def process(self, config=None, station=None):
        """Process all stations in the EventSummary.

        Args:
            config (dictionary): Config dictionary containing processing
                    parameters. Default is None. None results in using
                    get_config to find the user defined config or the default.
            station (str): Station to process. Default is None. None results in
                    all stations being processed.

        Raises:
            Exception: If there are not unprocessed streams.
        """
        if self.uncorrected_streams is None:
            raise Exception('There are no unprocessed streams to process.')

        # get config if none is supplied
        if config is None:
            config = get_config()

        ## TODO: Update to use amptools new process method after next release
        params = config['processing_parameters']
        taper_percentage = params['taper']['max_percentage']
        taper_type = params['taper']['type']

        if station is None:
            corrected_streams = {}
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for station_code in self.uncorrected_streams:
                    stream = self.uncorrected_streams[station_code]
                    for idx, trace in enumerate(stream):
                        trace.detrend('linear')
                        trace.detrend('demean')
                        trace.taper(max_percentage=taper_percentage,
                                type=taper_type)
                        trace.filter('highpass', freq=0.02, zerophase=True,
                                corners=4)
                        trace.detrend('linear')
                        trace.detrend('demean')
                    stream[idx] = trace
                    corrected_streams[station_code] = stream
            self.corrected_streams = corrected_streams
        else:
            if self.corrected_streams is None:
                warnings.warn('%r cannot be reprocessed, since the '
                'processed_streams dictionary has not been populated. Process '
                'all stream, then this station can be reprocessed.' % station)
                return
            if station not in self.uncorrected_streams:
                warnings.warn('%r is not an available station. Processing will '
                        'not continue.' % station)
                return
            stream = self.uncorrected_streams[station]
            corrected_streams = self.corrected_streams
            for idx, trace in enumerate(stream):
                trace.detrend('linear')
                trace.detrend('demean')
                trace.taper(max_percentage=taper_percentage,
                        type=taper_type)
                trace.filter('highpass', freq=0.02, zerophase=True,
                        corners=4)
                trace.detrend('linear')
                trace.detrend('demean')
            stream[idx] = trace
            corrected_streams[station] = stream
            self.corrected_streams = corrected_streams

    @property
    def uncorrected_streams(self):
        """
        Helper method for returning a dictionary of uncorrected streams.

        Returns:
            dictionary: Uncorrected streams (obspy.core.stream.Stream)
        """
        streams = copy.deepcopy(self._uncorrected_streams)
        return streams

    @uncorrected_streams.setter
    def uncorrected_streams(self, streams):
        """
        Helper method for setting a dictionary of uncorrected streams.

        Args:
            streams (dictionary): Uncorrected streams (obspy.core.stream.Stream)
        """
        if self.station_dict is None or len(streams) == len(self.station_dict):
            self._uncorrected_streams = streams
        else:
            warnings.warn('Stream dictionary is not the same length as the '
                    'number of stations. Setting failed.', Warning)

    def writeFlatfile(self, dataframe, output_directory):
        """
        Writes the flatfile dataframe as a csv file.

        Args:
            dataframe (pandas.DataFrame): Dataframe of flatfile.
            output_directory (str): Path to output directory.
        """
        # Create directory if it doesn't exist
        if not os.path.exists(output_directory):
                os.makedirs(output_directory)
        # Create file path
        today = datetime.datetime.now().strftime("%Y_%m_%d")
        filename = today + '_flatfile%s.csv'
        path = os.path.join(output_directory, filename)
        path = self._correctPath(path)
        dataframe.to_csv(path, mode = 'w', index=False)

    def writeParametric(self, directory):
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
        for station in streams:
            stream = streams[station]
            starttime = stream[0].stats['starttime'].strftime("%Y%m%d%H%M%S")
            extension = '.json'
            file = station + starttime + extension
            file_path = os.path.join(directory, file)
            geojson = self.getParametric(stream)
            with open(file_path,'wt') as f:
                json.dump(geojson, f)

    def writeStationTable(self, dataframe, output_directory, station):
        """
        Writes the station table as a csv file.

        Args:
            dataframe (pandas.DataFrame): Dataframe of flatfile.
            output_directory (str): Path to output directory.
            station (str): Station code.
        """
        # Create directory if it doesn't exist
        if not os.path.exists(output_directory):
                os.makedirs(output_directory)
        # Create file path
        filename = station + '%s.csv'
        path = os.path.join(output_directory, filename)
        path = self._correctPath(path)
        dataframe.to_csv(path, mode = 'w', index=False)

    def writeTimeseries(self, directory, file_format, include_json=True):
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
        for station in self.uncorrected_streams:
            stream = self.uncorrected_streams[station]
            starttime = stream[0].stats['starttime'].strftime("%Y%m%d%H%M%S")
            extension = '.' + file_format
            file = station + starttime + extension
            file_path = os.path.join(directory, file)
            stream.write(file_path, file_format)
        # parametric data will be required to use from_products
        if include_json is True:
            self.writeParametric(directory)

    def _cleanStats(self, stats):
        """
        Helper function for making dictionary json serializable.

        Args:
            stats (dict): Dictionary of stats.

        Returns:
            dictionary: Dictionary of cleaned stats.
        """
        for key, value in stats.items():
            if isinstance(value, (dict, AttribDict)):
                stats[key] = dict(self._cleanStats(value))
            elif isinstance(value, UTCDateTime):
                stats[key] = value.strftime(TIMEFMT)
            elif isinstance(value, float) and np.isnan(value) or value == '':
                stats[key] = 'null'
        return stats

    def _correctPath(self, path):
        """
        Helper to detect if a file already exists and append a number if it does.

        Args:
            path (str): Path to the csv file.

        Notes:
            This is only used by plot and table writers.
        """
        # Export csv
        file_number = ''
        while os.path.isfile(path % file_number):
            last = path % file_number
            file_number = int(file_number or 0) + 1
        path = path % file_number
        if file_number != "":
            print('%r already exists, writing to %r.' % (last, path))
        return path
