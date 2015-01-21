import sqlite3
import json
import subprocess
from os import path


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


if __name__ == "__main__":
    heatmap = TweetHeatMap("tweets.db")
    heatmap.heatmap()