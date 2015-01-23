from collections import defaultdict
import sqlite3
import json
import subprocess
from os import path
import datetime
import collections
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
from mpl_toolkits.basemap import Basemap
import pandas as pd
import matplotlib.patches as mpatches
import random

class TweetCoord:
    def __init__(self, dbname):
        self.dbname = dbname
        self.f_name = None

    def tweet_coord(self):
        """"""
        conn = sqlite3.connect(self.dbname)
        c = conn.cursor()

        coordinates = c.execute("SELECT COORDINATES "
                                "FROM TWEET, PLACE "
                                "WHERE TWEET.PLACE_ID = PLACE.PLACE_ID").fetchall()

        conn.close()
        return coordinates

    def tweet_coord_time_lang(self):
        """Recuperer coordonnees, temps et langue"""

        conn = sqlite3.connect(self.dbname)
        c = conn.cursor()

        coordinates = c.execute("SELECT COORDINATES, CREATED_AT, LANG "
                                "FROM TWEET, PLACE "
                                "WHERE TWEET.PLACE_ID = PLACE.PLACE_ID").fetchall()

        conn.close()
        return coordinates

    def sample_tweet_coord_time_lang(self, samplesize=10000):
        """Recuperer coordonnees, temps et langue"""

        conn = sqlite3.connect(self.dbname)
        c = conn.cursor()

        coordinates = c.execute("SELECT COORDINATES, CREATED_AT, LANG "
                                "FROM TWEET, PLACE "
                                "WHERE TWEET.PLACE_ID = PLACE.PLACE_ID").fetchall()

        conn.close()
        return random.sample(coordinates,samplesize)

    def distinct_lang(self):
        """"""

        conn = sqlite3.connect(self.dbname)
        c = conn.cursor()

        nlangs = c.execute("SELECT DISTINCT LANG FROM TWEET").fetchall()
        langs = [l[0] for l in nlangs]

        conn.close()
        return langs

    def coord_time(self):
        """Obtain tweet spatial coordinates along with the time of their creation"""
        conn = sqlite3.connect(self.dbname)
        c = conn.cursor()

        coordinates = c.execute("SELECT COORDINATES, CREATED_AT "
                                "FROM TWEET, PLACE "
                                "WHERE TWEET.PLACE_ID = PLACE.PLACE_ID").fetchall()

        conn.close()
        return coordinates

    def save_coord(self, f_name="coords"):
        """Save the coordinates of all tweets from the database in a 'f_name' file.
           In the file, the lat and long are separated by a whitespace, and a linebreak
           separates each pair of coordinates.

           Parameters:
           ----------
           f_name: str
                  Name of the file where the coordinates must be stored.
        """
        self.f_name = f_name
        coordinates = self.tweet_coord()

        with open(f_name, 'w') as output_file:
            for coord in coordinates:
                data = json.loads(coord[0])
                output_file.write("{} {}\n".format(data[1], data[0]))


class TweetHeatMap:
    def __init__(self, dbname, config=None):
        self.dbname = dbname
        self.coords = TweetCoord(dbname)
        self.coords.save_coord()
        coord_path = path.abspath(self.coords.f_name)
        self.config = {"-p": "-p {}".format(coord_path), "o": "-o heatmap.png", "width": "--width=2000",
                       "osm": "--osm", "B": "-B 0.8", "osm_base": "--osm_base=http://tile.openstreetmap.org"}

        if config is not None:
            self.config.update(config)

        self.heatmap_path = "./heatmap/heatmap.py"

    def heatmap(self):
        args = [self.heatmap_path]
        for value in self.config.itervalues():
            args.append(value)

        print(args)
        proc = subprocess.Popen(args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate()

        if stderr != "":
            raise Exception(stderr)

class VolumeTemps:
    """Tracer une courbe du nombre de tweets en fonction du temps et de la langue"""

    def __init__(self, dbname, langs, timedelta):
        self.dbname = dbname
        self.langs = langs
        self.distinct_langs = TweetCoord(dbname).distinct_lang()
        self.data = TweetCoord(dbname).tweet_coord_time_lang()
        self.aggregate = {}
        self.timedelta = timedelta

    def process(self):
        processed = []
        for tcl in self.data:
            ## Decode string format to python objects
            coords = json.loads(tcl[0])
            created_at = datetime.datetime.strptime(tcl[1], "%a %b %d %H:%M:%S +0000 %Y")
            lang = tcl[2]
            processed.append((coords, created_at, lang))

        ## Sort them by date
        time_ordered = sorted(processed, key=lambda x: x[1])

        ## Aggregate tweets in time windows of duration timedelta
        first_time = time_ordered[0][1]
        last_time = time_ordered[-1][1]

        time_win = [first_time]
        t = first_time
        while t < last_time:
            t += self.timedelta
            time_win.append(t)

        self.aggregate = pd.DataFrame(defaultdict.fromkeys(self.distinct_langs,defaultdict.fromkeys(time_win,0)))

        for e in time_ordered:
            if e[1] < first_time + self.timedelta:
                self.aggregate[e[2]][first_time] += 1
            else:
                while e[1] > first_time + self.timedelta:
                    first_time = first_time + self.timedelta

                self.aggregate[e[2]][first_time] += 1

    def plot_stacked(self):

        self.process()

        x = []
        y = {}
        patches=[]

        for l in langs:
            if l in self.aggregate:
                count = collections.OrderedDict(sorted(self.aggregate[l].iteritems()))
                x = count.keys()
                y[l] = count.values()

        ax = plt.stackplot(x,y.values())
        for i,poly in enumerate(ax):
            patches.append(mpatches.Patch(color=poly._facecolors[0], label=langs[i]))

        plt.legend(handles=patches)
        plt.show()


class AnimatedAggregatedTweets:
    """Create an animated map with tweets aggregated by time windows of timedelta units of time"""

    def __init__(self,dbname,timedelta,interval=200):
        self.dbname = dbname
        self.timedelta = timedelta # should be a datetime.timdelta object representing the size of the time window
        self.data = TweetCoord(dbname).sample_tweet_coord_time_lang()
        self.aggregate = []
        self.interval = interval # update animation each interval millisec
        self.time_win = []

    def time_window(self):
        """Aggregate tweets in time windows of timedelta time to monitor the temporal evolution"""

        processed = []
        for tcl in self.data:
            ## Decode string format to python objects
            coords = json.loads(tcl[0])
            created_at = datetime.datetime.strptime(tcl[1], "%a %b %d %H:%M:%S +0000 %Y")
            lang = tcl[2]
            processed.append((coords, created_at, lang))

        ## Sort them by date
        time_ordered = sorted(processed, key=lambda x: x[1])

        ## Aggregate tweets in time windows of duration timedelta
        first_time = time_ordered[0][1]
        last_time = time_ordered[-1][1]

        self.time_win = [first_time]
        t = first_time
        while t < last_time:
            t += self.timedelta
            self.time_win.append(t)

        for e in time_ordered:
            if e[1] < first_time + self.timedelta:
                self.aggregate.append((first_time, e[2], e[0]))
            else:
                while e[1] > first_time + self.timedelta:
                    first_time = first_time + self.timedelta

                self.aggregate.append((first_time, e[2], e[0]))

        self.aggregate = sorted(self.aggregate , key=lambda t: t[0])

    def animated_map(self, filename="movie.mp4"):
        """Plot the animated map"""
        self.time_window()

        map = Basemap(projection='cyl', resolution=None, lat_0=0., lon_0=0.)
        map.bluemarble()
        shade = None
        points = []

        def init():
            """subroutine to update points"""
            global shade
            time = self.time_win[0]
            lon = []
            lat = []
            for e in self.aggregate:
                if e[0] == time:
                    lon.append(e[2][0])
                    lat.append(e[2][1])

            x,y = map(0,0)
            for k in range(len(lon)):
                points.append(map.plot(x,y,'o', markerfacecolor = 'yellow', markeredgecolor= 'none' ,alpha=.5, markersize=3)[0])

            for pt,ln,lt in zip(points, lon, lat):
                x, y = map(ln,lt)
                pt.set_data(x, y)

            shade = map.nightshade(self.time_win[0],alpha=0.4)
            plt.title('%s (UTC)' % self.time_win[0])

            return points

        def animate(i):
            """subroutine to update points"""
            global shade
            time = self.time_win[i]
            lon = []
            lat = []
            for e in self.aggregate:
                if e[0] == time:
                    lon.append(e[2][0])
                    lat.append(e[2][1])

            for pt,ln,lt in zip(points, lon, lat):
                x, y = map(ln,lt)
                pt.set_data(x, y)

            for c in shade.collections:
                c.remove()
            shade = map.nightshade(self.time_win[i],alpha=0.4)

            plt.title('%s (UTC)' % self.time_win[i])
            return points

        frm = len(self.time_win)
        anim = animation.FuncAnimation(plt.gcf(), animate, range(frm), init_func=init, interval=self.interval)
        anim.save(filename)

        plt.show()

#if __name__ == "__main__":
#	delta = datetime.timedelta(0,0,0,0,10)
#	langs = ["fr","en","pt","in","ja","es"]
#	VolumeTemps("tweets.db",langs,delta).plot_stacked()

if __name__ == "__main__":
    delta = datetime.timedelta(0, 0, 0, 0, 30) # aggregate by one minute slices (we should use bigger delta with a bigger db)
    am = AnimatedAggregatedTweets("tweets.db", delta, 25)
    am.animated_map()

