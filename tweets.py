# -*- coding: utf-8 -*-

from __future__ import unicode_literals, division
from twitter import OAuth, TwitterStream
import sqlite3
import logging
from logging.handlers import RotatingFileHandler


# Creation of a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

file_handler = RotatingFileHandler('activity.log', 'a', 1000000, 1)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

steam_handler = logging.StreamHandler()
steam_handler.setLevel(logging.DEBUG)
logger.addHandler(steam_handler)

credentials = {"token": "2987172311-nww55Y0ZKPKhth05wkkX88bn5z6INqQRDBq5xSX",
               "token_secret": "digi83CDHjbD8vi8W8FnyLN7t8zd56pZ1XdqATdYJivex",
               "consumer_key": "V7xjnC1AdwECbbcv9OosDkawK",
               "consumer_secret": "rdEWrtop3r1ODjNCrPPpt18Z1Ey7BKtZRXJmwtTvQQ8u8JzULE"}


class AccessError(Exception):
    pass


class Tweet:
    def __init__(self, credentials):
        self.credentials = credentials
        self.api = None
        self.authenticated = False
        self.database = None

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

    def check_tweet(self, tweet):
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
        place_fields = ["country_code", "name", "id", "place_type"]

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
            logger.debug("The tweet failed the test because the 'place' field is not iterable.")
            return False

        for field in place_fields:
            if field not in tweet["place"]:
                logger.debug("The tweet failed the test because the {} field was "
                             "missing in tweet['place'].".format(field))
                return False

            if not isinstance(tweet["place"][field], unicode) or tweet["place"][field] == "":
                logger.debug("The tweet failed the test because the {} field in tweet['place'] was "
                             "incorrect: {}.".format(field, tweet["place"][field]))
                return False

        if tweet["place"]["place_type"] != 'city':
            logger.debug("The tweet failed the test because the 'place' is not a city.")
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
        self.database = dbname
        conn = sqlite3.connect(dbname)
        c = conn.cursor()

        try:
            c.execute("""CREATE TABLE PLACE
                         (PLACE_ID VARCHAR(50) NOT NULL PRIMARY KEY,
                          COUNTRY_CODE VARCHAR(5) NOT NULL,
                          NAME VARCHAR(50) NOT NULL);
                      """)

            c.execute("""CREATE TABLE TWEET
                         (TWEET_ID VARCHAR(50) NOT NULL PRIMARY KEY,
                          CREATED_AT VARCHAR(50) NOT NULL,
                          LANG VARCHAR (5) NOT NULL,
                          PLACE_ID VARCHAR(50) NOT NULL,
                          FOREIGN KEY(PLACE_ID) REFERENCES PLACE(PLACE_ID));
                     """)
        except sqlite3.IntegrityError:
            pass
        else:
            conn.commit()
        finally:
            conn.close()

    def record_tweet(self, tweet):
        """Record the input tweet in the given database.

        Parameters:
        ----------
        tweet: dictionary
              A dictionary corresponding to the tweet in the JSON format.
        """
        logger.info("Recording tweet {}.".format(tweet["id_str"]))
        if self.database is None:
            raise Exception("No database was created, please create a database before starting to record tweets.")

        conn = sqlite3.connect(self.database)
        c = conn.cursor()
        place_exists = c.execute("""SELECT * FROM PLACE WHERE PLACE_ID = ?""", (tweet["place"]["id"], )).fetchone()

        if place_exists is None:
            c.execute("""INSERT INTO PLACE VALUES(?, ?, ?)""", (tweet["place"]["id"],
                                                                tweet["place"]["country_code"],
                                                                tweet["place"]["name"]
                                                                ))

        c.execute("""INSERT INTO TWEET VALUES(?, ?, ?, ?)""", (tweet["id_str"],
                                                               tweet["created_at"],
                                                               tweet["lang"],
                                                               tweet["place"]["id"]
                                                               ))

        conn.commit()
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

    # for tweet in tweets_grabber.sample():
    #     if tweets_grabber.check_tweet(tweet):
    #         print(tweet["place"]["url"])