# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division
from twitter import OAuth, TwitterStream
import sqlite3
import numpy as np
import logging
from logging.handlers import RotatingFileHandler
import json

# Creation of a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

file_handler = RotatingFileHandler('activity.log', 'a', 1000000, 1)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# steam_handler = logging.StreamHandler()
# steam_handler.setLevel(logging.DEBUG)
# logger.addHandler(steam_handler)

credentials = {"token": "2987172311-nww55Y0ZKPKhth05wkkX88bn5z6INqQRDBq5xSX",
               "token_secret": "digi83CDHjbD8vi8W8FnyLN7t8zd56pZ1XdqATdYJivex",
               "consumer_key": "V7xjnC1AdwECbbcv9OosDkawK",
               "consumer_secret": "rdEWrtop3r1ODjNCrPPpt18Z1Ey7BKtZRXJmwtTvQQ8u8JzULE"}


def polygon_centroid(points):
    polygon = np.array(points, dtype=np.float64)
    return np.sum(polygon, axis=0) / polygon.shape[0]


class AccessError(Exception):
    pass


class Tweet:
    def __init__(self, credentials):
        self.credentials = credentials
        self.api = None
        self.authenticated = False
        self.dbname = None

    def authenticate(self):
        """Perform the authentication using the credentials given during the object instantiation."""
        if not self.authenticated:
            self.api = TwitterStream(auth=OAuth(**self.credentials))
            self.authenticated = True
            logging.info("Authenticated.")

    def sample(self):
        """Return the tweets sampled by the Twitter sample API.

           Returns:
           -------
           out: generator
               A generator returning the tweets in the JSON format.
        """

        if not self.authenticated:
            raise AccessError("You are not authenticated, please authenticate before streaming tweets.")
        else:
            return self.api.statuses.sample()

    @staticmethod
    def check_tweet(tweet):
        """Check whether the input tweet has all the necessary information to be saved and processed later on.

           Parameters:
           ----------
           tweet: dictionary
                 A dictionary corresponding to the tweet in the JSON format.

           Returns:
           -------
           out: boolean
               True if the tweet is conform, False otherwise
        """
        tweet_fields = ["lang", "place", "created_at", "id_str"]
        place_fields = ["country_code", "name", "id", "place_type", "bounding_box"]
        bounding_box_fields = ["type", "coordinates"]

        if tweet is None:
            logger.debug("The tweet is a NoneType object.")
            return False

        for field in tweet_fields:
            if field not in tweet:
                logger.debug("The tweet failed the test because the {} field was missing.".format(field))
                return False

            if not isinstance(tweet[field], unicode) or tweet[field] == "":
                if field != "place":
                    logger.debug("The tweet failed the test because the {} field "
                                 "was incorrect: {}".format(field, tweet[field]))
                    return False

        if not hasattr(tweet["place"], "__getitem__"):
            logger.debug("The tweet failed the test because the 'place' field is "
                         "not iterable: {}.".format(tweet["place"]))
            return False

        for field in place_fields:
            if field not in tweet["place"]:
                logger.debug("The tweet failed the test because the {} field was "
                             "missing in tweet['place'].".format(field))
                return False

            if field != "bounding_box":
                if not isinstance(tweet["place"][field], unicode) or tweet["place"][field] == "":
                    logger.debug("The tweet failed the test because the {} field in tweet['place'] was "
                                 "incorrect: {}.".format(field, tweet["place"][field]))
                    return False
            else:
                if not hasattr(tweet["place"]["bounding_box"], "__getitem__"):
                    logger.debug("The tweet failed the test because the 'bounding_box' field is not iterable.")
                    return False

        if tweet["place"]["place_type"] != 'city':
            logger.debug("The tweet failed the test because the 'place' is "
                         "not a city: {}.".format(tweet["place"]["place_type"]))
            return False

        for field in bounding_box_fields:
            if field not in tweet["place"]["bounding_box"]:
                logger.debug("The tweet failed the test because the {} field was "
                             "missing in tweet['place']['bounding_box'].".format(field))
                return False

        if tweet["place"]["bounding_box"]["type"] != "Polygon":
            logger.debug("The tweet failed the test because the 'bounding_box' is "
                         "not a Polygon: {}.".format(tweet["place"]["bounding_box"]))
            return False

        try:
            array = np.array(tweet["place"]["bounding_box"]["coordinates"], dtype=np.float64)
        except Exception as e:
            logger.info(e)
            return False

        if array.ndim != 3:
            logger.info("The 'coordinates' array is not of dimension 3: dimension {}".format(array.ndim))
            return False

        return True

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
            if place_exists is None:

                coordinates = polygon_centroid(tweet["place"]["bounding_box"]["coordinates"][0])
                coordinates = json.dumps(list(coordinates))
                c.execute("""INSERT INTO PLACE VALUES(?, ?, ?, ?)""", (tweet["place"]["id"],
                                                                       tweet["place"]["country_code"],
                                                                       tweet["place"]["name"],
                                                                       coordinates
                                                                       ))

            c.execute("""INSERT INTO TWEET VALUES(?, ?, ?, ?)""", (tweet["id_str"],
                                                                   tweet["created_at"],
                                                                   tweet["lang"],
                                                                   tweet["place"]["id"]
                                                                   ))
        except Exception as e:
            logging.warn("Exception raised during database writing:\n{}".format(e))
        else:
            conn.commit()
        finally:
            conn.close()

    def record(self):
        """Record the tweets from the sample Twitter API in the database."""
        logging.info("Starting recording tweets...")
        for tweet in self.sample():
            if self.check_tweet(tweet):
                self.record_tweet(tweet)


if __name__ == "__main__":
    tweets_grabber = Tweet(credentials)
    tweets_grabber.authenticate()
    tweets_grabber.create_database("tweets.db")
    tweets_grabber.record()