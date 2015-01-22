from __future__ import unicode_literals, division
from tweets import Tweet, polygon_centroid, log
from tweet_heatmap import TweetCoord
import sqlite3
import logging
import json

credentials = {'raphael': {"token": "2987172311-nww55Y0ZKPKhth05wkkX88bn5z6INqQRDBq5xSX",
                           "token_secret": "digi83CDHjbD8vi8W8FnyLN7t8zd56pZ1XdqATdYJivex",
                           "consumer_key": "V7xjnC1AdwECbbcv9OosDkawK",
                           "consumer_secret": "rdEWrtop3r1ODjNCrPPpt18Z1Ey7BKtZRXJmwtTvQQ8u8JzULE"},
               'martin': {'token': "2987208064-TbFv1uRAlKP9p3R3thm0eSoMnOLaK21aqoon5fi",
                          'token_secret': "GsLxcuHgUVReN7Fppnw4obyWB7StekgUpunYGuwEkmGxM",
                          'consumer_key': "ZhGac09sniZ0ni5DEnreCnATf",
                          'consumer_secret': "KvQJ4uQuHO2XeZULVgF2u1FIsbNNbDi4al9Pmj6fvZuDIB6WCL"}
               }


class LyonTweet(Tweet):
    def create_database(self, dbname):
        """Create the database where the tweet-related data are stored.

        Parameters:
        ----------
        dbname: str
               A string corresponding to the Sqlite file database where the data must be stored.
        """

        logging.info("Creating database {}.".format(dbname))
        self.dbname = dbname
        conn = sqlite3.connect(dbname)
        c = conn.cursor()

        c.execute("""CREATE TABLE PLACE
                         (PLACE_ID VARCHAR(50) NOT NULL PRIMARY KEY,
                          COUNTRY_CODE VARCHAR(5) NOT NULL,
                          NAME VARCHAR(50) NOT NULL,
                          COORDINATES VARCHAR(100) NOT NULL);
                      """)

        c.execute("""CREATE TABLE TWEET
                         (TWEET_ID VARCHAR(50) NOT NULL PRIMARY KEY,
                          CREATED_AT VARCHAR(50) NOT NULL,
                          LANG VARCHAR (5) NOT NULL,
                          PLACE_ID VARCHAR(50) NOT NULL,
                          COORDINATES VARCHAR(100) NOT NULL,
                          FOREIGN KEY(PLACE_ID) REFERENCES PLACE(PLACE_ID));
                     """)

    def record_tweet(self, tweet):
        """Record the input tweet in the given database.

        Parameters:
        ----------
        tweet: dictionary
              A dictionary corresponding to the tweet in the JSON format.
        """
        logger.info("Recording tweet {}.".format(tweet["id_str"]))
        if self.dbname is None:
            raise Exception("No database was created, please create a database before starting to record tweets.")

        conn = sqlite3.connect(self.dbname)
        c = conn.cursor()
        place_exists = c.execute("""SELECT * FROM PLACE WHERE PLACE_ID = ?""", (tweet["place"]["id"], )).fetchone()

        try:
            coordinates_tweet = json.dumps(tweet["coordinates"]["coordinates"])
            self.logger.debug(coordinates_tweet)

            if place_exists is None:

                coordinates = polygon_centroid(tweet["place"]["bounding_box"]["coordinates"][0])
                coordinates = json.dumps(list(coordinates))

                c.execute("""INSERT INTO PLACE VALUES(?, ?, ?, ?)""", (tweet["place"]["id"],
                                                                       tweet["place"]["country_code"],
                                                                       tweet["place"]["name"],
                                                                       coordinates
                                                                       ))

            c.execute("""INSERT INTO TWEET VALUES(?, ?, ?, ?, ?)""", (tweet["id_str"],
                                                                      tweet["created_at"],
                                                                      tweet["lang"],
                                                                      tweet["place"]["id"],
                                                                      coordinates_tweet
                                                                      ))
        except Exception as e:
            logging.warn("Exception raised during database writing:\n{}".format(e))
        else:
            conn.commit()
        finally:
            conn.close()

    def check_tweet(self, tweet):
        correct = Tweet.check_tweet(self, tweet)
        if not correct:
            return False

        if "coordinates" not in tweet:
            return False

        if not hasattr(tweet["coordinates"], "__getitem__"):
            return False
        return True


class LyonTweetCoord(TweetCoord):
    def tweet_coord(self):
        conn = sqlite3.connect(self.dbname)
        c = conn.cursor()

        coordinates = c.execute("SELECT COORDINATES FROM TWEET").fetchall()

        conn.close()
        return coordinates


if __name__ == "__main__":
    logger = log()
    tweets_grabber = LyonTweet(credentials["raphael"], logger)
    tweets_grabber.authenticate()
    tweets_grabber.create_database("tweets_lyon.db")
    tweets_grabber.record("filter", loc="4.746387, 45.719923, 4.913242, 45.789870")  # Coordinates of Lyon
    # lyon_tweets = LyonTweetCoord("tweets_lyon.db")
    # lyon_tweets.save_coord(f_name="coords_lyon")