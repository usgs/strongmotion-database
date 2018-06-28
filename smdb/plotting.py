# stdlib imports
import sys
import glob
import os.path

# third party imports
from obspy.core.stream import Stream
import matplotlib.pyplot as plt
from matplotlib.transforms import blended_transform_factory
from obspy.geodetics import gps2dist_azimuth

from amptools.io.cwb.core import read_cwb


def plot_moveout(streams, channel, eqlon, eqlat, plotfile):
    newstream = Stream()
    for stream in streams:
        newstream.append(stream.select(channel=channel)[0])
    for trace in newstream:
        lat = trace.stats.coordinates.latitude
        lon = trace.stats.coordinates.longitude
        trace.stats.distance = gps2dist_azimuth(lat, lon, eqlat, eqlon)[0]
    fig = plt.figure()
    newstream.plot(type='section', plot_dx=20e3, recordlength=100,
                   time_down=True, linewidth=.25, grid_linewidth=.25,
                   show=False, fig=fig)

    # Plot customization: Add station labels to offset axis
    ax = fig.axes[0]
    transform = blended_transform_factory(ax.transData, ax.transAxes)
    for trace in newstream:
        ax.text(trace.stats.distance / 1e3, 1.0, trace.stats.station,
                rotation=270, va="bottom", ha="center",
                transform=transform, zorder=10)
    plt.savefig(plotfile)


if __name__ == '__main__':
    indir = sys.argv[1]
    streams = []
    datfiles = glob.glob(os.path.join(indir, '*.dat'))
    for dfile in datfiles:
        stream = read_cwb(dfile)
        streams.append(stream)

    plotfile = os.path.join(os.path.expanduser('~'), 'testplot.png')
    plot_moveout(streams, 'HHE', 121.69, 24.14, plotfile)
