import sqlite3
import json


class TweetHeat:
    def __init__(self, dbname):
        self.dbname = dbname

    def save_coord(self, f_name="coords"):
        """Save the coordinates of all tweets from the database in a 'f_name' file.
           In the file, the lat and long are separated by a whitespace, and a linebreak
           separates each pair of coordinates.

           Parameters:
           ----------
           f_name: str
                  Name of the file where the coordinates must be stored.
        """
        conn = sqlite3.connect(self.dbname)
        c = conn.cursor()

        coordinates = c.execute("SELECT COORDINATES "
                                "FROM TWEET, PLACE "
                                "WHERE TWEET.PLACE_ID = PLACE.PLACE_ID")

        with open(f_name, 'w') as output_file:
            for coord in coordinates:
                data = json.loads(coord[0])
                output_file.write("{} {}\n".format(data[1], data[0]))


