from .pdl import get_params, store_params
from .cwb import store_stream, get_stream
from amptools.pgm.station_summary import StationSummary


class EventSummary2(object):

    @classmethod
    def fromEventID(self, eventsource, eventsourcecode, comcat_host=None):
        try:
            param_dict = get_params(eventsource, eventsourcecode,
                                    comcat_host=comcat_host)
            self._uncorrected_streams = []
            self._station_dict = {}
            for feature in param_dict['features']:
                if 'channels' not in feature['properties']:
                    continue
                channel_dict = feature['properties']['channels']
                channels = list(channel_dict.keys())
                channel1 = channels[0]
                stats = channel_dict[channel1]['stats']
                starttime = stats['starttime']
                endtime = stats['endtime']
                network = stats['network']
                station = stats['station']
                location = stats['location']
                stream = get_stream(network, station, location,
                                    starttime, endtime)
                self._uncorrected_streams.append(stream)
                # TODO - add class method to station

        except Exception as e:
            raise(e)
