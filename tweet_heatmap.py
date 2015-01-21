from collections import defaultdict
import sqlite3
import json
import subprocess
from os import path
import datetime
import time as time
import collections

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


def time_window(timedelta, time_coordinates):
    """Agregate tweets in time windows of timedelta time to monitor the temporal evolution"""
    processed = []
    for tc in time_coordinates:
        coords = json.loads(tc[0])
        created_at = datetime.datetime.strptime(tc[1], "%a %b %d %H:%M:%S +0000 %Y")
        processed.append((coords, created_at))

    time_ordered = sorted(processed, key=lambda x: x[1])

    count = defaultdict(list)
    first_time = time_ordered[0][1]

    for e in time_ordered:
        if e[1] < first_time + timedelta:
            if first_time not in count:
                count[first_time] = [e[0]]
            else:
                count[first_time].append(e[0])
        else:
            while e[1] > first_time + timedelta:
                first_time = first_time + timedelta
                count[first_time] = []

            count[first_time] = [e[0]]

    return count

t = TweetCoord("tweets.db")
ct = t.coord_time()
tw = datetime.timedelta(0,0,0,0,10,0) # one minute time delta
ct = time_window(tw,ct)

print ct.keys()
